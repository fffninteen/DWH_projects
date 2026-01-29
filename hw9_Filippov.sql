ALTER SCHEMA core_wrk RENAME TO dm_wrk;

CREATE OR REPLACE VIEW dm_wrk.v_d_currency AS
SELECT currency_id
     , source_system_id AS t_srs_id
     , CASE WHEN changed >= 3 THEN 'X' ELSE 'L' END::char(1) AS _t_row_status
     , COALESCE(start_date, '1900-01-01'::date) AS begin_date
     , COALESCE(final_date, '5000-01-01'::date) AS end_date
     , ''::varchar(64) AS currency_cd
     , name AS currency_nm
     , alpha_code AS okv_alpha_cd
     , ''::varchar(5) AS okv_numeric_cd
  FROM core.currency c;

CREATE OR REPLACE FUNCTION dm_wrk.f_dm_dq_buf_check(
    in_table_name character varying(128)
)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    v_table_name character varying(64);
    v_src_owner character varying(64);
    v_pk_cols text;
    v_pk_join_condition text;
    v_buf_full_name text; 
    out_sql text = '';
BEGIN
    v_table_name := substring(in_table_name, strpos(in_table_name, '.') + 1);
    v_src_owner := substring(in_table_name, 1, strpos(in_table_name, '.') - 1) || '_wrk';
    v_buf_full_name := v_src_owner || '._buf_' || v_table_name; 

    
    EXECUTE 'SELECT string_agg(column_name, '', '') FROM _meta_' || v_table_name || ' WHERE pk IS NOT NULL' 
    INTO v_pk_cols;

    
    EXECUTE 'SELECT string_agg(''a.''||column_name||'' = b.''||column_name, '' AND '') FROM _meta_' || v_table_name || ' WHERE pk IS NOT NULL'
    INTO v_pk_join_condition;

    IF v_pk_cols IS NULL OR v_pk_join_condition IS NULL THEN
        RETURN 'DO $inner$ BEGIN CALL meta.p_log(''dm_wrk.f_dm_dq_buf_check'', ''' || in_table_name || ''', 3, ''No PK found, skipping deduplication''); END $inner$;';
    END IF;

    
    out_sql := '
    DO $inner$
    DECLARE
        v_cnt bigint;
    BEGIN
        CALL meta.p_log(''dm_wrk.f_dm_dq_buf_check'', ''' || in_table_name || ''', 1, ''Start DQ check: Deduplication'');

        -- Подсчет дублей
        SELECT count(*) INTO v_cnt
          FROM (
              SELECT ' || v_pk_cols || '
                FROM ' || v_buf_full_name || ' -- Используем полное имя таблицы
               GROUP BY ' || v_pk_cols || '
              HAVING count(*) > 1
          ) t;
          
        CALL meta.p_log(''dm_wrk.f_dm_dq_buf_check'', ''' || in_table_name || ''', 2, ''Duplicate groups found'', v_cnt);

        -- Удаление дублей
        DELETE FROM ' || v_buf_full_name || ' a
         USING ' || v_buf_full_name || ' b
         WHERE a.ctid > b.ctid
           AND ' || v_pk_join_condition || ';

        GET DIAGNOSTICS v_cnt = ROW_COUNT;
        CALL meta.p_log(''dm_wrk.f_dm_dq_buf_check'', ''' || in_table_name || ''', 2, ''Deleted rows'', v_cnt);
        CALL meta.p_log(''dm_wrk.f_dm_dq_buf_check'', ''' || in_table_name || ''', 3, ''Finish'');
    END $inner$;
    ';

    RETURN out_sql;
END;
$$;

DO $$
DECLARE
    v_sql text;
BEGIN
    
    CALL dm_wrk.p_dm_prep_meta('dm.d_currency');

    
    v_sql := dm_wrk.f_dm_core_2_buf('dm.d_currency');
    EXECUTE v_sql;

    
    v_sql := dm_wrk.f_dm_dq_buf_check('dm.d_currency');
    
    
    RAISE NOTICE 'Generated SQL: %', v_sql;
    
    EXECUTE v_sql;
END $$;

DO $$
DECLARE
    v_sql text;
BEGIN
    
    CALL dm_wrk.p_dm_prep_meta('dm.d_currency');

    
    v_sql := dm_wrk.f_dm_core_2_buf('dm.d_currency');
    EXECUTE v_sql;

    
    v_sql := dm_wrk.f_dm_dq_buf_check('dm.d_currency');
    EXECUTE v_sql;
    
END $$;

CREATE OR REPLACE PROCEDURE dm_wrk.p_dm_merge_d_currency(
    in_table_name character varying
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_cnt_insert bigint;
    v_cnt_update bigint;
    v_error_text text;
    v_table_name character varying(64) = substring(in_table_name, strpos(in_table_name, '.') + 1);
    v_buf_full_name text = 'dm_wrk._buf_' || v_table_name;
    v_target_full_name text = in_table_name;
BEGIN
    CALL meta.p_log('dm_wrk.p_dm_merge_d_currency', in_table_name, 1, 'Start merging into target table');
    
    
    UPDATE dm.d_currency tgt 
    SET
        currency_cd = src.currency_cd,
        currency_nm = src.currency_nm,
        okv_alpha_cd = src.okv_alpha_cd,
        okv_numeric_cd = src.okv_numeric_cd,
        _t_row_status = src._t_row_status,
        begin_date = src.begin_date,
        end_date = src.end_date,
        _t_datetime = now() 
    FROM dm_wrk._buf_d_currency src
    WHERE tgt.currency_id = src.currency_id 
      AND tgt._t_row_status = 'L' 
      AND src._t_row_status = 'L' 
      AND (
          
          tgt.currency_nm IS DISTINCT FROM src.currency_nm OR
          tgt.okv_alpha_cd IS DISTINCT FROM src.okv_alpha_cd OR
          tgt.end_date IS DISTINCT FROM src.end_date
          
      );
    
    GET DIAGNOSTICS v_cnt_update = ROW_COUNT;
    CALL meta.p_log('dm_wrk.p_dm_merge_d_currency', in_table_name, 2, 'Updated rows', v_cnt_update);

    
    INSERT INTO dm.d_currency (
        currency_id, t_srs_id, _t_row_status, begin_date, end_date, 
        currency_cd, currency_nm, okv_alpha_cd, okv_numeric_cd,
        _t_datetime, _t_datetime_insert 
    )
    SELECT
        src.currency_id, src.t_srs_id, src._t_row_status, src.begin_date, src.end_date, 
        src.currency_cd, src.currency_nm, src.okv_alpha_cd, src.okv_numeric_cd,
        now(), now() 
    FROM dm_wrk._buf_d_currency src
    LEFT JOIN dm.d_currency tgt ON tgt.currency_id = src.currency_id
    WHERE tgt.currency_id IS NULL; 
    
    GET DIAGNOSTICS v_cnt_insert = ROW_COUNT;
    CALL meta.p_log('dm_wrk.p_dm_merge_d_currency', in_table_name, 2, 'Inserted rows', v_cnt_insert);
    CALL meta.p_log('dm_wrk.p_dm_merge_d_currency', in_table_name, 3, 'Finished merge');

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_error_text = MESSAGE_TEXT;
        CALL meta.p_log('dm_wrk.p_dm_merge_d_currency', in_table_name, 4, 'ERROR: ' || v_error_text);
        RAISE;
END;
$$;

DO $$
DECLARE
    v_sql text;
BEGIN
    
    CALL dm_wrk.p_dm_prep_meta('dm.d_currency');

    
    v_sql := dm_wrk.f_dm_core_2_buf('dm.d_currency');
    EXECUTE v_sql;

    
    v_sql := dm_wrk.f_dm_dq_buf_check('dm.d_currency');
    EXECUTE v_sql;
    
    
    CALL dm_wrk.p_dm_merge_d_currency('dm.d_currency');

END $$;



