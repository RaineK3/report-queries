with findCashTransactions as (
	--find CASH GL Transactions
		select 
			acEntries.TRN_REF_NO,
            acEntries.external_ref_no,
			acEntries.AC_BRANCH,
			acEntries.AC_NO,
			acEntries.AC_CCY,
			acEntries.TRN_CODE,
			acEntries.LCY_AMOUNT,
			'CASH' ModeOfPayment
		from fcubsfdb.acvw_all_ac_entries@ubs144.fdbbank.com acEntries
		left join fcubsfdb.gltm_glmaster@ubs144.fdbbank.com GL on acEntries.ac_no = GL.GL_CODE
		where acEntries.trn_dt between :P_Fdate and :P_Tdate
			and GL.TYPE = '5' --CASH GL
			and acEntries.TRN_CODE not in ('SWP') --Sweeping
            and acEntries.event not in ('REVR')
            and NOT EXISTS (
                SELECT *
                FROM fcubsfdb.acvw_all_ac_entries@ubs144.fdbbank.com ac
                WHERE ac.trn_ref_no = acEntries.trn_ref_no AND ac.CUST_GL = 'A' AND ac.event = 'REVR'
            ) -- exclude all pairs of reversal
--            and acEntries.trn_ref_no not in (select trn_ref_no from fcubsfdb.acvw_all_ac_entries@ubs144.fdbbank.com where event = 'REVR')
)

select 

    temp.ZONE ZONE,
    temp.AC_CCY CURRENCY,
    temp.TYPE,
    temp.SOURCE,
    sum(temp.AMOUNT) AMOUNT_UNIT,
    sum(temp.AMOUNT_IN_MMK) EQUIVALENT_MMK,
    0 EQUIVALENT_USD
from ( 
    select
        acEntries.TRN_REF_NO,
        acEntries.AC_NO,
        case
            when brnGroup.BRANCH_GROUP_CODE = 'YGN' then 'Yangon'
            when brnGroup.BRANCH_GROUP_CODE = 'MDY' then 'Mandalay'
            when brnGroup.BRANCH_GROUP_CODE = 'NPT' then 'Naypyitaw'
            else 'Other Cities'
        end ZONE,
        acEntries.AC_CCY,
        case 
            when acEntries.DRCR_IND = 'C' then 'Cash_In'
            when acEntries.DRCR_IND = 'D' then 'Cash_Out'
        end TYPE,
        case
            when acEntries.TRN_CODE in ('CDO','CLD','CLR','CLW','CWF','CWO','CWR','ECM','FNC','POS') then 'ATM'
            else 'Branch'
        end SOURCE,
        case
            when acEntries.AC_CCY = 'MMK' then acEntries.LCY_AMOUNT
            else FCY_AMOUNT
        end AMOUNT,
        acEntries.LCY_AMOUNT AMOUNT_IN_MMK 
    from fcubsfdb.acvw_all_ac_entries@ubs144.fdbbank.com acEntries
    inner join findCashTransactions on findCashTransactions.TRN_REF_NO = acEntries.TRN_REF_NO 
        and findCashTransactions.external_ref_no = acEntries.external_ref_no
        and findCashTransactions.AC_BRANCH = acEntries.AC_BRANCH 
        and findCashTransactions.AC_CCY = acEntries.AC_CCY
        and findCashTransactions.LCY_AMOUNT = acEntries.LCY_AMOUNT
    inner join branchcommon.SRV_TM_BC_BRANCH_GRP_DETAIL brnGroup on brnGroup.BRANCH_CODE = acEntries.AC_BRANCH
    inner join branchcommon.SRV_TM_BC_BRANCH_GRP_MASTER brnGroupMaster on brnGroupMaster.BRANCH_GROUP_CODE = brnGroup.BRANCH_GROUP_CODE
        and brnGroupMaster.RECORD_STAT = 'O'
    where acEntries.trn_dt between :P_Fdate and :P_Tdate
        and acEntries.CUST_GL = 'A'
) temp
group by temp.ZONE, temp.AC_CCY, temp.TYPE, temp.SOURCE;