with findCashTransactions as (
	--find CASH GL Transactions
		select 
			acEntries.TRN_REF_NO,
			acEntries.AC_BRANCH,
			acEntries.AC_NO,
			acEntries.AC_CCY,
			acEntries.TRN_CODE,
			acEntries.LCY_AMOUNT,
			'CASH' ModeOfPayment
		from fcubsfdb.acvw_all_ac_entries@ubs144.fdbbank.com acEntries
		left join fcubsfdb.gltm_glmaster@ubs144.fdbbank.com GL on acEntries.ac_no = GL.GL_CODE
		where acEntries.trn_dt = '21-May-24'
			and GL.TYPE = '5' --CASH GL
			and acEntries.TRN_CODE != 'SWP' --Sweeping
)

select  
	tempPreFinal.AC_CCY Currency,
	tempPreFinal.totalOpenAcc - tempPreFinal.totalCloseAcc NumberOfAccount,

	tempPreFinal.depositInCash_EquivalentMMK DepositInCash_EquivalentMMK, 
	tempPreFinal.depositInTransfer_EquivalentMMK DepositInTransfer_EquivalentMMK,
	0 DepositInClearing_EquivalentMMK,    
	tempPreFinal.depositInCash_CurrencyUnit DepositInCash_CurrencyUnit,
	tempPreFinal.depositInTransfer_CurrencyUnit DepositInTransfer_CurrencyUnit,
	0 DepositInClearing_CurrencyUnit,

	tempPreFinal.depositOutCash_EquivalentMMK DepositOutCash_EquivalentMMK,
	tempPreFinal.depositOutTransfer_EquivalentMMK DepositOutTransfer_EquivalentMMK,
	0 DepositOutClearing_EquivalentMMK,
	tempPreFinal.depositOutCash_CurrencyUnit DepositOutCash_CurrencyUnit,
	tempPreFinal.depositOutTransfer_CurrencyUnit DepositOutTransfer_CurrencyUnit,
	0 DepositOutClearing_CurrencyUnit
from (
    select 
        temp.AC_CCY,
		( 
			select count(acc_no) 
			from(
				select acc.ac_open_date open_date,acc.cust_ac_no acc_no
				from fcubsfdb.sttm_cust_account@ubs144.fdbbank.com acc
				inner join fcubsfdb.sttm_account_balance@ubs144.fdbbank.com bal
				on acc.cust_ac_no = bal.cust_ac_no
				where acc.ac_open_date = '21-May-24' and (bal.acy_withdrawable_bal + bal.acy_blocked_amt) > 0
			union all
			select open_date,acc_no 
			from
				(select min(accEnt.trn_dt) open_date,acc.cust_ac_no acc_no
				from fcubsfdb.sttm_cust_account@ubs144.fdbbank.com acc
                inner join fcubsfdb.sttm_account_balance@ubs144.fdbbank.com bal
                on acc.cust_ac_no = bal.cust_ac_no
                left join fcubsfdb.acvw_all_ac_entries@ubs144.fdbbank.com accEnt
                on acc.cust_ac_no = accEnt.ac_no
                where acc.ac_open_date <> '21-May-24'
                group by acc.cust_ac_no)
                where open_date = '21-May-24'
			)
		) totalOpenAcc,
		(
			select count(AC_NO) from 
            FCUBSFDB.STTB_CUST_AC_CLOSURE@ubs144.fdbbank.com Clo
            inner join FCUBSFDB.STTM_CUST_ACCOUNT@ubs144.fdbbank.com Acc
            on Clo.ac_no = Acc.cust_ac_no
			where Clo.CLOSING_DATE =  '21-May-24'
            and Clo.CLOSING_DATE <> Acc.AC_OPEN_DATE
		) totalCloseAcc,    

		temp.depositInCash_EquivalentMMK,
		temp.depositInTransfer_EquivalentMMK,
		temp.depositOutCash_EquivalentMMK,
		temp.depositOutTransfer_EquivalentMMK,

		temp.depositInCash_CurrencyUnit,
		temp.depositInTransfer_CurrencyUnit,
		temp.depositOutCash_CurrencyUnit,
		temp.depositOutTransfer_CurrencyUnit    
    from (
        select 
            TOTAL_TRANS.AC_CCY,
            sum(TOTAL_TRANS.depositInCash_EquivalentMMK) depositInCash_EquivalentMMK,
            sum(TOTAL_TRANS.depositInTransfer_EquivalentMMK) depositInTransfer_EquivalentMMK,
            sum(TOTAL_TRANS.depositOutCash_EquivalentMMK) depositOutCash_EquivalentMMK,
            sum(TOTAL_TRANS.depositOutTransfer_EquivalentMMK) depositOutTransfer_EquivalentMMK,    
            sum(TOTAL_TRANS.depositInCash_CurrencyUnit) depositInCash_CurrencyUnit,
            sum(TOTAL_TRANS.depositInTransfer_CurrencyUnit) depositInTransfer_CurrencyUnit,
            sum(TOTAL_TRANS.depositOutCash_CurrencyUnit) depositOutCash_CurrencyUnit,
            sum(TOTAL_TRANS.depositOutTransfer_CurrencyUnit) depositOutTransfer_CurrencyUnit
        from ( 
            select
                TRANS.AC_NO,
                TRANS.AC_CCY,
                TRANS.TRN_CODE,
                case
                    when TRANS.DRCR_IND = 'C' and TRANS.MODEOFPAYMENT = 'CASH' then TRANS.LCY_AMOUNT
                    else 0
                end depositInCash_EquivalentMMK,
                case
                    when TRANS.DRCR_IND = 'C' and TRANS.MODEOFPAYMENT = 'TRANSFER' then TRANS.LCY_AMOUNT
                    else 0
                end depositInTransfer_EquivalentMMK,
                
                case
                    when TRANS.DRCR_IND = 'D' and TRANS.MODEOFPAYMENT = 'CASH' then TRANS.LCY_AMOUNT
                    else 0
                end depositOutCash_EquivalentMMK,
                case
                    when TRANS.DRCR_IND = 'D' and TRANS.MODEOFPAYMENT = 'TRANSFER' then TRANS.LCY_AMOUNT
                    else 0
                end depositOutTransfer_EquivalentMMK,
                
                case
                    when TRANS.DRCR_IND = 'C' and TRANS.MODEOFPAYMENT = 'CASH' and TRANS.AC_CCY != 'MMK' then TRANS.FCY_AMOUNT
                    else 0
                end depositInCash_CurrencyUnit,
                case
                    when TRANS.DRCR_IND = 'C' and TRANS.MODEOFPAYMENT = 'TRANSFER' and TRANS.AC_CCY != 'MMK' then TRANS.FCY_AMOUNT
                    else 0
                end depositInTransfer_CurrencyUnit,
                
                case
                    when TRANS.DRCR_IND = 'D' and TRANS.MODEOFPAYMENT = 'CASH' and TRANS.AC_CCY != 'MMK' then TRANS.FCY_AMOUNT
                    else 0
                end depositOutCash_CurrencyUnit,
                case
                    when TRANS.DRCR_IND = 'D' and TRANS.MODEOFPAYMENT = 'TRANSFER' and TRANS.AC_CCY != 'MMK' then TRANS.FCY_AMOUNT
                    else 0
                end depositOutTransfer_CurrencyUnit
            from ( 
                select 
                    acEntries.AC_NO,
                    acEntries.AC_CCY,
                    acEntries.DRCR_IND,
                    acEntries.TRN_CODE,
                    acEntries.LCY_AMOUNT,
                    acEntries.FCY_AMOUNT,
                    nvl(findCashTransactions.ModeOfPayment, 'TRANSFER') ModeOfPayment
                    
                from FCUBSFDB.ACVW_ALL_AC_ENTRIES@ubs144.fdbbank.com acEntries
                left join fcubsfdb.sttb_account_ca@ubs144 acc on acEntries.ac_no = acc.ac_gl_no
                left join findCashTransactions on findCashTransactions.TRN_REF_NO = acEntries.TRN_REF_NO 
                    and findCashTransactions.AC_BRANCH = acEntries.AC_BRANCH 
                    and findCashTransactions.AC_CCY = acEntries.AC_CCY
                    and findCashTransactions.LCY_AMOUNT = acEntries.LCY_AMOUNT
                where acEntries.CUST_GL = 'A' and acEntries.trn_dt = '21-May-24'
            ) TRANS
        ) TOTAL_TRANS
        group by TOTAL_TRANS.AC_CCY
    ) temp
) tempPreFinal	;


	

