--9-July-24
--modify  reversal removing transaction (trn_code)
--adding one fixed deposit gl to concat tenor_days(21050010001)

WITH ds AS (
	SELECT
	    t.trn_ref_no,
		t.ac_entry_sr_no,
	    t.ac_branch,
	    CASE
			 WHEN t.gl_code IN ('21050010001')  and t.orig_tenor_days <= 40
				THEN CONCAT(t.gl_code, '(' || 30 || ')')
            WHEN t.gl_code IN ('21050010001')  and (t.orig_tenor_days > 40 and t.orig_tenor_days <= 60)
				THEN CONCAT(t.gl_code, '(' || 60 || ')')
			WHEN t.gl_code IN ('21030010001', '21130010001', '21230010001','21050010001') 
				THEN CONCAT(t.gl_code, '(' || t.orig_tenor_days || ')')
	        ELSE t.gl_code
		END gl_code,
	    t.drcr_ind,
	    t.trn_code,
	    t.ac_ccy,
	    t.fcy_amount,
	    t.exch_rate,
	    t.lcy_amount,
	    t.trn_dt,
	    t.value_dt,
	    t.amount_tag,
	    gl.type
	FROM (
	    SELECT ac_entry.*, 
            NVL(ac_gl.ac_natural_gl, ac_entry.ac_no) AS gl_code, 
            i.orig_tenor_days
		FROM fcubsfdb.acvw_all_ac_entries ac_entry
		LEFT JOIN fcubsfdb.sttb_account_ca ac_gl ON ac_gl.ac_gl_no = ac_entry.ac_no
		LEFT JOIN fcubsfdb.ictm_acc i ON i.acc = ac_entry.ac_no
		WHERE 
            ac_entry.trn_dt = :P_Date 
            AND ac_entry.auth_stat = 'A' -- only authorzied records
            AND ac_entry.trn_code NOT IN('MRG') -- not migrated transactions
            AND NOT (ac_entry.ac_no = '15060010001' AND ac_entry.trn_code IN ('SWP')) -- excluded sweep transactions in IBS
            -- for year end transaction, extract only event = YEND
            AND ac_entry.event NOT IN ('REVR', 'YEND') -- excluded reversal and year end transactions
            AND NOT EXISTS (
                SELECT *
                FROM fcubsfdb.acvw_all_ac_entries ac1
                WHERE ac1.trn_ref_no = ac_entry.trn_ref_no AND ac1.auth_stat = 'A' AND ac1.event = 'REVR' and ac1.trn_code = ac_entry.trn_code
            ) -- exclude all pairs of reversal
	) t
	INNER JOIN fcubsfdb.gltm_glmaster gl
	    ON gl.gl_code = t.gl_code
)

SELECT 
    gl_code AS GL_CODE, 
    ac_ccy AS AC_CCY, 
    SUM(CASE
            WHEN trn_mode = 'cash' AND ((drcr_ind = 'D' AND lcy_amount >= 0) OR (drcr_ind = 'C' AND lcy_amount < 0)) THEN ABS(lcy_amount)
            ELSE 0
        END) AS CASHDEBIT_EQUIVALENTMMK,
    SUM(CASE
            WHEN trn_mode = 'transfer' AND ((drcr_ind = 'D' AND lcy_amount >= 0) OR (drcr_ind = 'C' AND lcy_amount < 0)) THEN ABS(lcy_amount)
            ELSE 0
        END) AS TRANSFERDEBIT_EQUIVALENTMMK,
    0 AS CLEARDEBIT_EQUIVALENTMMK,
    SUM(CASE
            WHEN trn_mode = 'cash' AND ((drcr_ind = 'C' AND lcy_amount >= 0) OR (drcr_ind = 'D' AND lcy_amount < 0)) THEN ABS(lcy_amount)
            ELSE 0
        END) AS CASHCREDIT_EQUIVALENTMMK,
    SUM(CASE
            WHEN trn_mode = 'transfer' AND ((drcr_ind = 'C' AND lcy_amount >= 0) OR (drcr_ind = 'D' AND lcy_amount < 0)) THEN ABS(lcy_amount)
            ELSE 0
        END) AS TRANSFERCREDIT_EQUIVALENTMMK,
    0 AS CLEARCREDIT_EQUIVALENTMMK,
    SUM(CASE
            WHEN ac_ccy <> 'MMK' AND trn_mode = 'cash' AND ((drcr_ind = 'D' AND fcy_amount >= 0) OR (drcr_ind = 'C' AND fcy_amount < 0)) THEN ABS(fcy_amount)
            ELSE 0
        END) AS CASHDEBIT_CURRENCYUNIT,
    SUM(CASE
            WHEN ac_ccy <> 'MMK' AND trn_mode = 'transfer' AND ((drcr_ind = 'D' AND fcy_amount >= 0) OR (drcr_ind = 'C' AND fcy_amount < 0)) THEN ABS(fcy_amount)
            ELSE 0
        END) AS TRANSFERDEBIT_CURRENCYUNIT,
    0 AS CLEARDEBIT_CURRENCYUNIT,
    SUM(CASE
            WHEN ac_ccy <> 'MMK' AND trn_mode = 'cash' AND ((drcr_ind = 'C' AND fcy_amount >= 0) OR (drcr_ind = 'D' AND fcy_amount < 0)) THEN ABS(fcy_amount)
            ELSE 0
        END) AS CASHCREDIT_CURRENCYUNIT,
    SUM(CASE
            WHEN ac_ccy <> 'MMK' AND trn_mode = 'transfer' AND ((drcr_ind = 'C' AND fcy_amount >= 0) OR (drcr_ind = 'D' AND fcy_amount < 0)) THEN ABS(fcy_amount)
            ELSE 0
        END) AS TRANSFERCREDIT_CURRENCYUNIT,
    0 AS CLEARCREDIT_CURRENCYUNIT,
    0 AS CASHDEBIT_EQUIVALENTUSD,
    0 AS TRANSFERDEBIT_EQUIVALENTUSD,
    0 AS CLEARDEBIT_EQUIVALENTUSD,
    0 AS CASHCREDIT_EQUIVALENTUSD,
    0 AS TRANSFERCREDIT_EQUIVALENTUSD,
    0 AS CLEARCREDIT_EQUIVALENTUSD
FROM (
    SELECT gl_code, ac_ccy, drcr_ind, trn_mode, SUM(lcy_amount) AS lcy_amount, SUM(fcy_amount) AS fcy_amount
    FROM (
        SELECT d1.trn_ref_no, d1.trn_dt, d1.gl_code, d1.drcr_ind, d1.ac_ccy, d1.lcy_amount, d1.fcy_amount, d1.trn_code, 'cash' AS trn_mode
        FROM ds d1
        WHERE d1.type = 5 OR 
            EXISTS (
                SELECT d2.*
                FROM ds d2
                WHERE d2.trn_ref_no = d1.trn_ref_no 
                    AND d2.lcy_amount = d1.lcy_amount 
                    AND d2.ac_ccy = d1.ac_ccy 
                    AND d2.ac_branch = d1.ac_branch
                    AND d2.type = 5)

        UNION ALL

        SELECT d1.trn_ref_no, d1.trn_dt, d1.gl_code, d1.drcr_ind, d1.ac_ccy, d1.lcy_amount, d1.fcy_amount, d1.trn_code, 'transfer' AS trn_mode
        FROM ds d1
        WHERE NOT EXISTS (
            SELECT d2.*
            FROM ds d2
            WHERE d2.trn_ref_no = d1.trn_ref_no 
                AND d2.lcy_amount = d1.lcy_amount 
                AND d2.ac_ccy = d1.ac_ccy 
                AND d2.ac_branch = d1.ac_branch
                AND d2.type = 5)
    ) tmp1
    GROUP BY gl_code, ac_ccy, drcr_ind, trn_mode
) tmp2
GROUP BY gl_code, ac_ccy
ORDER BY gl_code, ac_ccy;