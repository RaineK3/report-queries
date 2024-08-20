SELECT
    CASE
        WHEN cust_det.customer_type = 'I' AND ds.joint_ac_indicator = 'S' THEN 'Individual'
        WHEN cust_det.customer_type = 'I' AND ds.joint_ac_indicator = 'J' THEN 'Joint'
        WHEN cust_det.customer_type = 'C' THEN 'Corporate'
    END AS "Type",
    ds.ac_open_date AS "Open Date",
    ds.cust_ac_no AS "Account No",
    ds.cust_no AS "CIF No",
    NVL(cust_det.customer_prefix, '') AS "Prefix",
    cust_det.full_name AS "Name",
    cust_det.date_of_birth AS "DOB",
    cust_det.DESIGNATION AS "DESIGNATION",
    cust_det.unique_id_name AS "ID Card Type",
    cust_det.unique_id_value AS "ID Card No",
    NVL(cust_det.telephone, cust_det.mobile_number) AS "Phone No",
    REPLACE(
        cust_det.address_line1 || ', ' || cust_det.address_line2 || 
        CASE 
            WHEN cust_det.address_line2 IS NOT NULL THEN ', '
            ELSE ''
        END || cust_det.address_line3 || ', ' || cust_det.address_line4,
        ',,',
        ','
    ) AS "Address",
    ds.branch_code AS "Branch Code",
    brn.branch_name AS "Branch Name"
FROM (
    SELECT branch_code, cust_ac_no, cust_no, ac_open_date, joint_ac_indicator
    FROM fcubsfdb.sttm_cust_account

    UNION ALL
    
    SELECT a.branch_code, a.cust_ac_no, b.joint_holder_code AS cust_no, a.ac_open_date, a.joint_ac_indicator
    FROM fcubsfdb.sttm_cust_account a
    INNER JOIN fcubsfdb.sttm_acc_joint_holder b
        ON a.cust_ac_no = b.cust_ac_no
    WHERE a.joint_ac_indicator = 'J'
) ds
INNER JOIN (
    SELECT 
        a.customer_no, a.customer_type, a.full_name, b.customer_prefix, 
        CASE
            WHEN a.customer_type = 'I' THEN  b.date_of_birth
            ELSE c.incorp_date
        END date_of_birth, 
        a.unique_id_name, a.unique_id_value, 
        a.address_line1, a.address_line2, a.address_line3, a.address_line4, b.telephone, b.mobile_number,d.DESIGNATION
    FROM fcubsfdb.sttm_customer a
    LEFT JOIN fcubsfdb.sttm_cust_personal b ON a.customer_no = b.customer_no
    LEFT JOIN fcubsfdb.sttm_cust_corporate c ON c.customer_no = a.customer_no
    LEFT JOIN FCUBSFDB.sttm_cust_professional d on d.customer_no = a.customer_no
    ) cust_det ON cust_det.customer_no = ds.cust_no --AND cust_det.unique_id_name = 'NRCS'
INNER JOIN fcubsfdb.sttm_branch brn
    ON brn.branch_code = ds.branch_code
WHERE ds.ac_open_date BETWEEN '12-Aug-24' AND '18-Aug-24'
and substr(cust_ac_no,6,1) <> 5 -- exclude NOSTRO A/C
ORDER BY ds.ac_open_date, ds.cust_ac_no, ds.cust_ac_no;

select * from fcubsfdb.cstb_field_labels;

    