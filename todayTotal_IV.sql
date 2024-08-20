select
temp.branch_code,
temp.openingDate ,
--class.account_class,
class.description,
case 
    when  temp.openingDate between :P_Fdate and :P_Tdate and temp.closingDate is null
      then count(temp.Acc_No)
    else 0
end totalOpened,
case 
    when  temp.closingDate between :P_Fdate and :P_Tdate
      then count(temp.Acc_No)
    else 0
end totalClosed
	from
	(select 
	nvl(open.branch_code,close.branch_code) branch_code,
	nvl(open.ac_open_date,close.ac_open_date) openingDate,
	nvl(close.closing_date,'') closingDate,
	nvl(open.cust_ac_no,close.ac_no) Acc_No,
	nvl(open.account_class,close.account_class) acc_Class
		from
			(select acc.ac_open_date,acc.cust_ac_no,acc.account_class,acc.branch_code
			from fcubsfdb.sttm_cust_account acc 
			inner join fcubsfdb.sttm_account_balance bal
			on bal.cust_ac_no = acc.cust_ac_no
			where acc.ac_open_date between :P_Fdate and :P_Tdate 
            and (bal.acy_withdrawable_bal + bal.acy_blocked_amt) > 0
            UNION ALL
            select ac_open_date,cust_acc_no ,account_class,branch_code
            from 
                (select min(accEnt.trn_dt) ac_open_date,acc.cust_ac_no cust_acc_no,acc.account_class,acc.branch_code
                    from fcubsfdb.sttm_cust_account acc
                    inner join fcubsfdb.sttm_account_balance bal
                    on acc.cust_ac_no = bal.cust_ac_no
                    left join fcubsfdb.acvw_all_ac_entries accEnt
                    on acc.cust_ac_no = accEnt.ac_no
                    where acc.ac_open_date not between :P_Fdate and :P_Tdate
                    group by acc.cust_ac_no,acc.account_class,acc.branch_code)
                    where ac_open_date between :P_Fdate and :P_Tdate
            )open
			
		full join (select ac.ac_open_date,clo.ac_no,clo.closing_date,ac.branch_code,ac.account_class
			from fcubsfdb.sttb_cust_ac_closure clo 
			inner join fcubsfdb.sttm_cust_account ac
			on ac.cust_ac_no = clo.ac_no
			where clo.closing_date between :P_Fdate and :P_Tdate
            and clo.closing_date <> ac.ac_open_date) close
			on close.ac_no = open.cust_ac_no
	)temp
	inner join(select account_class,description from fcubsfdb.sttm_account_class) class 
	on class.account_class = temp.acc_Class 
 group by temp.branch_code,temp.openingDate,temp.closingDate,class.description--,class.account_class
 order by branch_code;
 
 --(21050010001(22))