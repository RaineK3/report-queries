with dates as(
select distinct value_date as loop_date 
from fcubsfdb.cstb_all_dates
where value_date < :P_Date 
and value_date not in (select distinct holiday_date from fcubsfdb.cltb_affected_holidays)
and to_char(value_date,'Day') not like 'S%'     
and value_date <> '22-MAY-24'   --not maintain this day as a holiday in system
order by value_date desc
fetch first 5 rows only
)
,
bkgdate as (select
    loop_date,
    max(bkg_date) as max_bkg_date,
    account,
    branch_code
  from
    dates DT
  left join (
    select bkg_date, account, branch_code
    from fcubsfdb.actb_accbal_history
    where account ='11010010001' or ( account like '21%' and substr(account,9,3) <> '000' ) 
    group by account, branch_code, bkg_date
  )
--  on bkg_date < DT.loop_date
on ( account like '21%' AND bkg_date <= DT.loop_date) or
    (account like '11%' AND bkg_date < DT.loop_date) 
  group by loop_date, account, branch_code
  order by loop_date
)

--select * from bkgdate;
,
  
Opening as (select b.loop_date,a.branch_code,abs(a.lcy_opening_bal)lcy_opening_bal
from (
 select trn_dt, branch_code, sum(lcy_opening_bal) AS lcy_opening_bal
  from (
    select
      sum(acy_opening_bal) AS acy_opening_bal,
      sum(lcy_opening_bal) AS lcy_opening_bal,
      branch_code,
      bkg_date AS trn_dt
    from fcubsfdb.actb_accbal_history 
    where account ='11010010001'
    and (bkg_date, account, branch_code) IN (select loop_date, account, branch_code from bkgdate) 
    group by  branch_code, bkg_date
  )
  group by  branch_code, trn_dt
) a
inner join (select * from dates) b 
on a.trn_dt = b.loop_date
--where (a.trn_dt,b.loop_date) in (select max_bkg_date,loop_date from bkgdate)
order by b.loop_date,a.branch_code
)
--select * from opening;
,
CashInOut as (select  loop_date,trn_dt,ac_branch,branch_name
        ,sum(nvl(case when drcr_ind='C' then LCY_AMOUNT end,0)) CashIn
        ,sum(nvl(case when drcr_ind='D' then -1*LCY_AMOUNT end,0)) CashOut    
from(
select  trn_ref_no,ac_entry_sr_no,event,ac_branch,ac_no,b.ac_gl_no,nvl(b.ac_natural_gl,a.ac_no) ac_natural_gl,
ac_ccy,category,drcr_ind,trn_code,lcy_amount,fcy_amount,exch_rate,trn_dt,value_dt,amount_tag,related_account,related_customer,
        module,cust_gl,user_id,auth_id,external_ref_no,org_source_ref,source_code,b.gl_aclass_type,c.branch_name,loop_date
from fcubsfdb.acvw_all_ac_entries a, fcubsfdb.sttb_account b ,fcubsfdb.sttm_branch c , dates dt
where a.ac_no = b.ac_gl_no
and a.ac_branch = c.branch_code
and a.trn_dt = dt.loop_date
and trn_ref_no not in (select trn_ref_no from fcubsfdb.acvw_all_ac_entries where event = 'REVR')
and (a.trn_ref_no,a.ac_branch) in (select trn_ref_no,ac_branch from fcubsfdb.acvw_all_ac_entries where ac_no like '1101%')
)
where (substr(ac_natural_gl,1,4) in ('1104','1105','1406','1512','1503','1505','1508','2121','2122','2123','2124','2101','2111','2104','2114','2102','2112','1501','7261','1506','2660','2601','2630','2640','2670') or substr(ac_natural_gl,1,1) in ('6','7') )
group by loop_date,trn_dt, ac_branch,branch_name
order by loop_date,ac_branch
)
--select * from CashInOut;
,


Deposit as (select  trn_dt,branch_code
       , sum(nvl(case when type='2' then lcy_opening_bal end,0)) current_bal
       , sum(nvl(case when type='1' then lcy_opening_bal end,0)) saving_bal
       , sum(nvl(case when type='4' then lcy_opening_bal end,0)) call_bal
       , sum(nvl(case when type='3' then lcy_opening_bal end,0)) fixed_bal
       , sum(lcy_opening_bal) total_dep
from (
    select
      sum(acy_closing_bal) AS acy_opening_bal,
      sum(lcy_closing_bal) AS lcy_opening_bal,
      branch_code,account,substr(account,4,1) type,
      bkg_date AS trn_dt
    from fcubsfdb.actb_accbal_history 
    where account like '21%'
--    and (bkg_date, account, branch_code) IN (select max_bkg_date, account, branch_code from bkgdate) 
    group by  branch_code, bkg_date,account
    )
	where type <> 0
    group by trn_dt,branch_code
    order by trn_dt,branch_code
	),
--    select * from deposit;
DepBKG as(
    select * from (  
    select TRN_DT, BRANCH_CODE BRANCH, CURRENT_BAL, SAVING_BAL, CALL_BAL, FIXED_BAL, TOTAL_DEP, max(TRNDT) over(partition by TRN_DT) MAX_TRN_DT,TRNDT,(TOTAL_DEP - BKG_TOTAL_DEP) DAILY_NET_DEPOSIT_AMT
    from (
    select dep.TRN_DT, dep.BRANCH_CODE, dep.CURRENT_BAL, dep.SAVING_BAL, dep.CALL_BAL, dep.FIXED_BAL, dep.TOTAL_DEP ,dep1.trn_dt TRNDT, dep1.branch_code bc,dep1.TOTAL_DEP BKG_TOTAL_DEP
    from
    (select * from deposit where trn_dt in (select * from dates) ) dep
    inner join
    (select * from deposit dep
    where trn_dt in (select value_date from fcubsfdb.cstb_all_dates where value_date < :P_Date and to_char(value_date,'Day') not like 'S%' and value_date <> '22-MAY-24'
                    order by value_date desc
                    fetch first 6 rows only) 
     ) dep1
    on dep.branch_code = dep1.branch_code
    where dep.trn_dt > dep1.trn_dt
    order by dep.trn_dt
    )
    )
    where MAX_TRN_DT = TRNDT
)

select case when BRANCH_CODE <> '001' then '' else to_char(LOOP_DATE,'DD-MON-YY') end as LOOP_DATE
        , BRANCH_CODE, BRANCH_NAME, LCY_OPENING_BAL, CASHIN, CASHOUT, NET_AMOUNT, LCY_CLOSING_BAL, CDR
        , DAILY_NET_DEPOSIT_AMT, TOTAL_DEP, CURRENT_BAL, CUR_DEPOSIT_RATIO, SAVING_BAL, SAV_DEPOSIT_RATIO
        , CALL_BAL, CALL_DEPOSIT_RATIO, FIXED_BAL, FIXED_DEPOSIT_RATIO, CCDR
FROM(        
select LOOP_DATE, BRANCH_CODE,BRANCH_NAME, LCY_OPENING_BAL, CASHIN, CASHOUT, NET_AMOUNT, LCY_CLOSING_BAL,
       case when total_dep <> 0 then round((lcy_closing_bal / total_dep) ,2) else 0 end CDR,DAILY_NET_DEPOSIT_AMT,TOTAL_DEP
       ,CURRENT_BAL,case when total_dep <> 0 then round((current_bal / total_dep) ,2) else 0 end Cur_Deposit_Ratio
       ,SAVING_BAL, case when total_dep <> 0 then round((saving_bal / total_dep) ,2) else 0 end Sav_Deposit_Ratio
       ,CALL_BAL,case when total_dep <> 0 then round((call_bal / total_dep) ,2) else 0 end Call_Deposit_Ratio
       ,FIXED_BAL, case when total_dep <> 0 then round((fixed_bal / total_dep) ,2) else 0 end Fixed_Deposit_Ratio
       ,case when call_bal <> 0 then round((lcy_closing_bal / call_bal) ,2) else 0 end CCDR
from(  
       
select CashLimit.*,CURRENT_BAL, SAVING_BAL, CALL_BAL, FIXED_BAL, TOTAL_DEP,DAILY_NET_DEPOSIT_AMT
from
(
select Opening.Loop_date,Opening.branch_code,Brn.BRANCH_NAME,Opening.lcy_opening_bal,CashInOut.cashin,CashInOut.cashout
    ,CashInOut.cashin + CashInOut.cashout Net_Amount, Opening.lcy_opening_bal + CashInOut.cashin + CashInOut.cashout lcy_closing_bal
     from Opening , CashInOut , fcubsfdb.sttm_branch brn
     where Opening.Loop_date = CashInOut.trn_dt
     and Opening.branch_code = CashInOut.ac_branch 
     and brn.BRANCH_CODE = CashInOut.ac_branch )CashLimit
     inner join DepBKG Deposit
 on CashLimit.Loop_date = Deposit.trn_dt
 where CashLimit.branch_code = Deposit.BRANCH
 and Deposit.branch <> '999'
 
 union all
 
  select trn_dt,branch,'FARMERS DEVELOPMENT BANK - MDY (HO)' BRANCH_NAME,0 LCY_OPENING_BAL, 0 CASHIN, 0 CASHOUT, 0 NET_AMOUNT, 0 LCY_CLOSING_BAL
    ,CURRENT_BAL, SAVING_BAL, CALL_BAL, FIXED_BAL, TOTAL_DEP, DAILY_NET_DEPOSIT_AMT
  from DepBKG where BRANCH = '999'
  )
 order by to_date(Loop_date),branch_code
 )