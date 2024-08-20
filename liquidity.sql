select * from(
 select 
            preTemp.CODE,
            preTemp.CURRENCY,
            abs(preTemp.AMOUNT) AMOUNT,
            abs(preTemp.AMOUNT_EQUIVALENTMMK) AMOUNT_EQUIVALENTMMK
        from (
        select 
            temp.CODE,
            temp.CURRENCY,
            temp.CREDIT_AMOUNT - temp.DEBIT_AMOUNT AMOUNT,
            temp.CREDIT_AMOUNT_EQUIVALENTMMK - temp.DEBIT_AMOUNT_EQUIVALENTMMK AMOUNT_EQUIVALENTMMK
        from (
                select 
                    TRANS.GL_CODE CODE,
                    TRANS.AC_CCY CURRENCY,
                    case 
                        when TRANS.AC_CCY = 'MMK' then nvl(sum(TRANS.DEBIT_LCY), 0)
                        else nvl(sum(DEBIT_FCY), 0)
                    end DEBIT_AMOUNT,
                    case 
                        when TRANS.AC_CCY = 'MMK' then nvl(sum(TRANS.CREDIT_LCY), 0)
                        else nvl(sum(CREDIT_FCY), 0)
                    end CREDIT_AMOUNT,
                    nvl(sum(TRANS.DEBIT_LCY), 0) DEBIT_AMOUNT_EQUIVALENTMMK,
                    nvl(sum(TRANS.CREDIT_LCY), 0) CREDIT_AMOUNT_EQUIVALENTMMK
                from (
                        select 
                            acEntries.TRN_REF_NO,
                            acEntries.AC_BRANCH,
                            case
                                when acEntries.CUST_GL = 'A' and acc.AC_NATURAL_GL in ('21030010001','21130010001','21230010001') and ictmAcc.ORIG_TENOR_DAYS is not null 
                                    then concat(acc.AC_NATURAL_GL, '(' || ictmAcc.ORIG_TENOR_DAYS || ')')
                                when acEntries.CUST_GL = 'A' and acc.AC_NATURAL_GL in ('21030010001','21130010001','21230010001') and ictmAcc.ORIG_TENOR_MONTHS is not null 
                                    then concat(acc.AC_NATURAL_GL, '(' || ictmAcc.ORIG_TENOR_MONTHS || ')')
                                when acEntries.CUST_GL = 'A' and acc.AC_NATURAL_GL in ('21030010001','21130010001','21230010001') and ictmAcc.ORIG_TENOR_YEARS is not null 
                                    then concat(acc.AC_NATURAL_GL, '(' || ictmAcc.ORIG_TENOR_YEARS || ')')
                                when acEntries.CUST_GL = 'A'
                                    then acc.AC_NATURAL_GL
                                when acEntries.CUST_GL = 'G'
                                    then acEntries.ac_no
                            end GL_CODE,
                            acEntries.AC_CCY,
                            acEntries.DRCR_IND,
                            acEntries.TRN_CODE,
                            acEntries.LCY_AMOUNT,
                            case
                                when acEntries.DRCR_IND = 'D' then acEntries.LCY_AMOUNT
                            end DEBIT_LCY,
                            case
                                when acEntries.DRCR_IND = 'C' then acEntries.LCY_AMOUNT
                            end CREDIT_LCY,                    
                            acEntries.FCY_AMOUNT,
                            case
                                when acEntries.DRCR_IND = 'D' then acEntries.FCY_AMOUNT
                            end DEBIT_FCY,
                            case
                                when acEntries.DRCR_IND = 'C' then acEntries.FCY_AMOUNT
                            end CREDIT_FCY,  
                            acEntries.EXCH_RATE,
                            acEntries.TRN_DT
                        from fcubsfdb.acvw_all_ac_entries@ubs144 acEntries
                        left join fcubsfdb.sttb_account_ca@ubs144 acc on acEntries.ac_no = acc.ac_gl_no
                        left join fcubsfdb.ictm_acc@ubs144 ictmAcc on ictmAcc.acc = acc.ac_gl_no
                        where acEntries.TRN_DT <= '28-Mar-24'
                             and acEntries.auth_stat = 'A'
                             --and acc.AC_NATURAL_GL = '21030010001'
                    ) TRANS
                    group by TRANS.GL_CODE, TRANS.AC_CCY
                ) temp
            ) preTemp
    ) where code IN ('91000010001')