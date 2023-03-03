#########################################################################
# Third stage of FCQ admin setup/course audit sequence
# L:\mgt\FCQ\R_Code\FCQ_Audit03.R - Vince Darcangelo, 9/19/22
#########################################################################

# TOC (order of operations)
# Part IV:  Set administration dates                     (~ 2 mins)
# Part V:   Generate audit files                         (fcq_audit04.r)

#########################################################################
#'*Part IV: Set administration dates*
#########################################################################
# not sure if the conversion is necessary, but it is possible
# date <- as.Date(n14$mtgStartDt, format = '%m/%d/%Y')
# format(date, '%d%b%Y')

# exclude/include classes manually through adminInd
n15 <- n14 %>%
  mutate(adminInd = case_when(
# beijing: update each semester
    INSTITUTION_CD == 'CUDEN' & LOC_ID == 'IC_BEIJING' ~ 0,
    paste(SBJCT_CD, CATALOG_NBR, sep = '-') == 'BMSC-7806' ~ 0,
    paste(SBJCT_CD, CATALOG_NBR, sep = '-') == 'BMSC-7810' ~ 0,
    SBJCT_CD %in% c('AMBA', 'CLSC', 'EMEA', 'XBUS') ~ 0,
    SBJCT_CD == 'CMFT' & CATALOG_NBR %in% c('5910', '5911') ~ 0,
    paste(SBJCT_CD, CATALOG_NBR, CLASS_SECTION_CD, sep = '-') == 'INTS-3939-901' ~ 1,
    paste(SBJCT_CD, CATALOG_NBR, CLASS_SECTION_CD, sep = '-') %in% c('ENVM-5034-001', 'ENVM-5034-002') ~ 0,
    TRUE ~ adminInd
  ))

# set administration dates
n16 <- n15 %>%
  mutate(adminDtTxt = case_when(
    adminInd == 0 ~ '',

# adminDtTxt exceptions
  campus == 'BD' & SBJCT_CD == 'LAWS' & CATALOG_NBR == '7102' ~ 'Mar 20-Mar 24',
# BCOR mod a
   adminInd== 1 & campus == 'BD' & ACAD_GRP_CD == 'BUSN' &
     SESSION_CD == 'B81' ~ 'Feb 27-Mar 03',
  campus == 'CE' & SBJCT_CD == 'MBAE' & CATALOG_NBR %in% c('6002', '6022', '6092', '6212', '6480', '6565') ~ 'Feb 06-Feb 10',
  SBJCT_CD == 'NCLL' & SESSION_CD == 'DC1' ~ 'Feb 27-Mar 03',
# BD winter session
  adminInd == 1 & SESSION_CD == 'BWS' ~ 'Jan 02-Jan 06',
# NCLL courses
  adminInd== 1 & SBJCT_CD == 'NCLL' & SESSION_CD == 'DC1' ~ 'Feb 27-Mar 03',
# DN-EDUC changes
  campus == 'DN' & paste(SBJCT_CD, CATALOG_NBR, CLASS_SECTION_CD, sep = '-') %in% c('LCRT-6910-901', 'SPSY-6917-901') ~ 'Apr 24-May 02',

###############################
# exception examples to use
  # BD winter session
#    adminInd == 1 & SESSION_CD == 'BWS' ~ 'Jan 02-Jan 06',
  # BD-ENGR-ASEN
#    adminInd== 1 & between(mtgEndDt,'04/20/2022','05/31/2022')
#      & SBJCT_CD == 'ASEN' & CATALOG_NBR == '1400' ~ 'Apr 18-Apr 29',
  # BD-ENGR-MCEN
#    adminInd== 1 & between(mtgEndDt,'04/20/2022','05/31/2022')
#      & SBJCT_CD == 'MCEN' & CATALOG_NBR == '4085' ~ 'Apr 18-Apr 29',
#   adminInd== 1 & campus == 'DN' & ACAD_ORG_CD == 'D-EDUC' &
#     SESSION_CD == 'DMR' ~ 'May 02-May 10',
#   adminInd== 1 & SBJCT_CD == 'NCLL' & SESSION_CD == 'DC1' ~ 'Feb 28-Mar 04',
#   adminInd== 1 & campus == 'CE' & SBJCT_CD == 'MBAE' & SESSION_CD == 'BM1'
#     ~ 'Feb 07-Feb 11',
#   TRUE ~ 'TBD'
# ))
################################

# standard administration dates
  adminInd==1 & between(fcqEnDt,'01/11/2023','01/17/2023') ~ 'Jan 09-Jan 13',
  adminInd==1 & between(fcqEnDt,'01/18/2023','01/24/2023') ~ 'Jan 17-Jan 21',
  adminInd==1 & between(fcqEnDt,'01/25/2023','01/31/2023') ~ 'Jan 23-Jan 27',
  adminInd==1 & between(fcqEnDt,'02/01/2023','02/07/2023') ~ 'Jan 30-Feb 03',
  adminInd==1 & between(fcqEnDt,'02/08/2023','02/14/2023') ~ 'Feb 06-Feb 10',
  adminInd==1 & between(fcqEnDt,'02/15/2023','02/21/2023') ~ 'Feb 13-Feb 17',
  adminInd==1 & between(fcqEnDt,'02/22/2023','02/28/2023') ~ 'Feb 20-Feb 24',
  adminInd==1 & between(fcqEnDt,'03/01/2023','03/07/2023') ~ 'Feb 27-Mar 03',
  adminInd==1 & between(fcqEnDt,'03/08/2023','03/14/2023') ~ 'Mar 06-Mar 10',
  adminInd==1 & between(fcqEnDt,'03/15/2023','03/21/2023') ~ 'Mar 13-Mar 17',
  adminInd==1 & between(fcqEnDt,'03/22/2023','04/04/2023') & campus %in% c('BD', 'CE', 'B3') ~ 'Mar 20-Mar 24',
  adminInd==1 & between(fcqEnDt,'03/22/2023','04/04/2023') & campus %in% c('DN', 'MC') ~ 'Mar 27-Mar 31',
  adminInd==1 & between(fcqEnDt,'04/05/2023','04/11/2023') ~ 'Apr 03-Apr 07',
  adminInd==1 & between(fcqEnDt,'04/12/2023','04/18/2023') ~ 'Apr 10-Apr 14',
  adminInd==1 & between(fcqEnDt,'04/19/2023','04/25/2023') ~ 'Apr 17-Apr 21',
  adminInd==1 & between(fcqEnDt,'04/26/2023','05/02/2023') & campus == 'MC' & CAMPUS_CD == 'AMC' ~ 'Apr 24-Apr 28',
  # ENGR Extended Final
  adminInd==1 & between(fcqEnDt,'04/26/2023','05/31/2023') & campus=='BD' & SBJCT_CD=='GEEN' & CATALOG_NBR %in% c('1017', '1400', '2010', '2400', '3024', '3400', '3853') ~ 'Apr 24-May 04',
  # Boulder/CEPS Final
  adminInd==1 & between(fcqEnDt,'04/26/2023','05/31/2023') & campus %in% c('BD', 'CE') ~ 'Apr 24-May 02',
  # B3 Final
  adminInd==1 & between(fcqEnDt,'04/26/2023','05/31/2023') & campus == 'B3' ~ 'Apr 24-May 02',
  # Denver Final
  adminInd==1 & between(fcqEnDt,'04/26/2023','05/31/2023') & campus == 'DN' & CAMPUS_CD !='AMC' ~ 'Apr 24-May 02',
  # Anschutz Final
  adminInd==1 & between(fcqEnDt,'05/03/2023','05/31/2023') & campus == 'MC' & CAMPUS_CD=='AMC' ~ 'May 01-May 09'
  ))

#XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX#
# search for dups with diff end dts
n16x <- n16 %>%
  ungroup() %>%
  select(CLASS_NUM, adminDtTxt) %>%
  distinct() %>%
  filter(adminDtTxt != '') %>%
  group_by(CLASS_NUM) %>%
  filter(n() >= 2)
#XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX#

# create and format adminStartDt/adminEndDt columns
n17 <- n16 %>%
  mutate(adminStartDt = case_when(
    adminDtTxt != '' ~ substr(adminDtTxt, 1, 6),
    TRUE ~ '')) %>%
  mutate(adminEndDt = case_when(
    adminDtTxt != '' ~ substr(adminDtTxt, 8, 13),
    TRUE ~ '')) %>%
  mutate(adminStartDt = str_replace_all(adminStartDt, ' ', '-')) %>%
  mutate(adminEndDt = str_replace_all(adminEndDt, ' ', '-')) %>%
  mutate(adminStartDt = format(as.Date(adminStartDt, format = '%b-%d'), '%m/%d/%Y')) %>%
  mutate(adminEndDt = format(as.Date(adminEndDt, format = '%b-%d'), '%m/%d/%Y'))

clscu <- n17 %>%
  select(campus,deptOrgID,fcqdept,TERM_CD,INSTITUTION_CD,CAMPUS_CD,SESSION_CD,SBJCT_CD,CATALOG_NBR,CLASS_SECTION_CD,instrNum,instrNm,instrLastNm,instrFirstNm,instrMiddleNm,instrPersonID,instrConstituentID,instrEmplid,instrEmailAddr,INSTRCTR_ROLE_CD,ASSOCIATED_CLASS,assoc_class_secID,GRADE_BASIS_CD,totEnrl_nowd_comb,totEnrl_nowd,ENRL_TOT,ENRL_CAP,ROOM_CAP_REQUEST,CRSE_LD,LOC_ID,CLASS_STAT,crseSec_comp_cd,SSR_COMP_CD,combStat,spons_AcadGrp,spons_AcadOrg,spons_fcqdept,spons_deptOrgID,spons_id,SCTN_CMBND_CD,SCTN_CMBND_LD,INSTRCTN_MODE_CD,CLASS_TYPE,SCHED_PRINT_INSTR,mtgStartDt,mtgEndDt,CLASS_START_DT,CLASS_END_DT,CLASS_NUM,ACAD_GRP_CD,ACAD_GRP_LD,ACAD_ORG_CD,ACAD_ORG_LD,totSchedPrintInstr_Y,totSchedPrintInstr_Not_Y,totInstrPerClass,totCrsePerInstr,totAssocClassPerInstr,indLEC,indNotLEC,totLECInstr,totNotLECInstr,cmbsecInfo,adminInd,fcqNote,indInstrNm,indDIS_IND_THE,indDEPT_RQST,indIndptStdy,indMinEnrl,indCombSect0,indCandDegr,indGiveOnlyLEC,indCrseTooLate,fcqStDt,fcqEnDt,adminDtTxt,adminStartDt,adminEndDt)

clscu <- clscu %>%
  mutate(fcqNote = case_when(
    adminInd == 0 & fcqNote == '' ~ 'No FCQs per department request',
    TRUE ~ fcqNote
  ))

#########################################################################
# attach custom questions - generate code with custQ_parse.R
#########################################################################
clscu2 <- clscu %>%
  mutate(attr1 = case_when(
    campus == 'BD' & SBJCT_CD == 'AREN' & CATALOG_NBR >= '1000' & CATALOG_NBR <= '4999' ~ 'bd-a-aren-a',
    campus == 'BD' & SBJCT_CD == 'ASEN' & CATALOG_NBR >= '1000' & CATALOG_NBR <= '4999' ~ 'bd-a-asen-a',
    campus == 'BD' & SBJCT_CD == 'BMEN' & CATALOG_NBR >= '1000' & CATALOG_NBR <= '6999' ~ 'bd-a-bmen-a',
    campus == 'BD' & SBJCT_CD == 'CHEN' & CATALOG_NBR >= '4810' & CATALOG_NBR <= '4820' ~ 'bd-a-chen-a,bd-a-chen-b',
    campus == 'BD' & SBJCT_CD == 'CHEN' & CATALOG_NBR >= '1000' & CATALOG_NBR <= '4999' ~ 'bd-a-chen-a',
    campus == 'BD' & SBJCT_CD == 'COEN' & CATALOG_NBR >= '1000' & CATALOG_NBR <= '4999' ~ 'bd-a-coen-a',
    campus == 'BD' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '1000' & CATALOG_NBR <= '4999' ~ 'bd-a-csci-a',
    campus == 'BD' & SBJCT_CD == 'CVEN' & CATALOG_NBR >= '1000' & CATALOG_NBR <= '4999' ~ 'bd-a-cven-d',
    campus == 'BD' & SBJCT_CD == 'ECEN' & CATALOG_NBR >= '1000' & CATALOG_NBR <= '4999' ~ 'bd-a-ecen-b',
    campus == 'BD' & SBJCT_CD == 'EVEN' & CATALOG_NBR >= '1000' & CATALOG_NBR <= '8000' ~ 'bd-a-even-a',
    campus == 'BD' & SBJCT_CD == 'GEEN' & CATALOG_NBR >= '1000' & CATALOG_NBR <= '4999' ~ 'bd-a-geen-a',
    campus == 'BD' & SBJCT_CD == 'ENES' & CATALOG_NBR >= '1000' & CATALOG_NBR <= '4999' ~ 'bd-a-huen-a',
    campus == 'BD' & SBJCT_CD == 'MCEN' & CATALOG_NBR >= '1000' & CATALOG_NBR <= '4999' ~ 'bd-a-mcen-w',
    campus == 'BD' & SBJCT_CD == 'ASIA' & CATALOG_NBR >= '1000' & CATALOG_NBR <= '6999' ~ 'bd-c-asia-a',
    campus == 'BD' & SBJCT_CD == 'CMCI' & CATALOG_NBR >= '1010' & CATALOG_NBR <= '1010' ~ 'bd-c-cmci-a',
    campus == 'BD' & SBJCT_CD == 'EDUC' & CATALOG_NBR >= '2020' & CATALOG_NBR <= '2050' ~ 'bd-c-educ-a',
    campus == 'BD' & SBJCT_CD == 'EDUC' & CATALOG_NBR >= '3013' & CATALOG_NBR <= '3013' ~ 'bd-c-educ-b',
    campus == 'BD' & SBJCT_CD == 'EDUC' & CATALOG_NBR >= '4023' & CATALOG_NBR <= '4112' ~ 'bd-c-educ-c',
    campus == 'BD' & SBJCT_CD == 'EDUC' & CATALOG_NBR >= '4125' & CATALOG_NBR <= '4125' ~ 'bd-c-educ-d',
    campus == 'BD' & SBJCT_CD == 'EDUC' & CATALOG_NBR >= '4222' & CATALOG_NBR <= '4411' ~ 'bd-c-educ-e',
    campus == 'BD' & SBJCT_CD == 'EDUC' & CATALOG_NBR >= '5005' & CATALOG_NBR <= '5060' ~ 'bd-c-educ-f',
    campus == 'BD' & SBJCT_CD == 'EDUC' & CATALOG_NBR >= '5205' & CATALOG_NBR <= '5295' ~ 'bd-c-educ-g',
    campus == 'BD' & SBJCT_CD == 'EDUC' & CATALOG_NBR >= '5315' & CATALOG_NBR <= '5545' ~ 'bd-c-educ-h',
    campus == 'BD' & SBJCT_CD == 'EDUC' & CATALOG_NBR >= '5605' & CATALOG_NBR <= '5635' ~ 'bd-c-educ-i',
    campus == 'BD' & SBJCT_CD == 'EDUC' & CATALOG_NBR >= '6368' & CATALOG_NBR <= '6368' ~ 'bd-c-educ-j',
    campus == 'BD' & SBJCT_CD == 'ENGL' & CATALOG_NBR >= '1000' & CATALOG_NBR <= '4999' ~ 'bd-c-engl-a',
    campus == 'BD' & SBJCT_CD == 'ENVM' & CATALOG_NBR >= '6002' & CATALOG_NBR <= '6002' ~ 'bd-c-envm-a',
    campus == 'BD' & SBJCT_CD == 'ENES' & CATALOG_NBR >= '3350' & CATALOG_NBR <= '3350' ~ 'bd-c-huen-a',
    campus == 'BD' & SBJCT_CD == 'INVS' & CATALOG_NBR >= '2989' & CATALOG_NBR <= '2989' ~ 'bd-c-invs-a',
    campus == 'BD' & SBJCT_CD == 'IPHY' & CATALOG_NBR >= '3415' & CATALOG_NBR <= '3415' ~ 'bd-c-iphy-b',
    campus == 'BD' & SBJCT_CD == 'IPHY' & CATALOG_NBR >= '3435' & CATALOG_NBR <= '3435' ~ 'bd-c-iphy-c',
    campus == 'BD' & SBJCT_CD == 'ITAL' & CATALOG_NBR >= '1010' & CATALOG_NBR <= '1010' ~ 'bd-c-ital-a',
    campus == 'BD' & SBJCT_CD == 'ITAL' & CATALOG_NBR >= '1020' & CATALOG_NBR <= '1020' ~ 'bd-c-ital-b',
    campus == 'BD' & SBJCT_CD == 'ITAL' & CATALOG_NBR >= '1050' & CATALOG_NBR <= '1050' ~ 'bd-c-ital-c',
    campus == 'BD' & SBJCT_CD == 'ITAL' & CATALOG_NBR >= '2110' & CATALOG_NBR <= '2110' ~ 'bd-c-ital-d',
    campus == 'BD' & SBJCT_CD == 'ITAL' & CATALOG_NBR >= '2120' & CATALOG_NBR <= '2120' ~ 'bd-c-ital-e',
    campus == 'BD' & SBJCT_CD == 'PMUS' & CATALOG_NBR >= '1636' & CATALOG_NBR <= '1636' ~ 'bd-c-pmus-a,bd-c-pmus-b',
    campus == 'BD' & SBJCT_CD == 'PMUS' & CATALOG_NBR >= '1000' & CATALOG_NBR <= '6999' ~ 'bd-c-pmus-b',
    campus == 'BD' & SBJCT_CD == 'ATOC' & CATALOG_NBR >= '1000' & CATALOG_NBR <= '1999' ~ 'bd-d-atoc-a',
    campus == 'CE' & SBJCT_CD == 'ECON' & CATALOG_NBR >= '1000' & CATALOG_NBR <= '6999' ~ 'ce-c-econ-a',
    campus == 'CE' & SBJCT_CD == 'ESLG' & CATALOG_NBR >= '1130' & CATALOG_NBR <= '1130' ~ 'ce-c-eslg-a',
    campus == 'CE' & SBJCT_CD == 'ESLG' & CATALOG_NBR >= '1140' & CATALOG_NBR <= '1140' ~ 'ce-c-eslg-b',
    campus == 'CE' & SBJCT_CD == 'ESLG' & CATALOG_NBR >= '1210' & CATALOG_NBR <= '1210' ~ 'ce-c-eslg-c',
    campus == 'CE' & SBJCT_CD == 'ESLG' & CATALOG_NBR >= '1222' & CATALOG_NBR <= '1222' ~ 'ce-c-eslg-d',
    campus == 'CE' & SBJCT_CD == 'ESLG' & CATALOG_NBR >= '1410' & CATALOG_NBR <= '1410' ~ 'ce-c-eslg-e',
    campus == 'CE' & SBJCT_CD == 'NCIE' & CATALOG_NBR >= '1000' & CATALOG_NBR <= '6999' ~ 'ce-c-ncie-a',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '3415' & CATALOG_NBR <= '3415' ~ 'dn-a-csci-aa',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '3453' & CATALOG_NBR <= '3453' ~ 'dn-a-csci-ab',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '3508' & CATALOG_NBR <= '3508' ~ 'dn-a-csci-ac',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '3511' & CATALOG_NBR <= '3511' ~ 'dn-a-csci-ad',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '3560' & CATALOG_NBR <= '3560' ~ 'dn-a-csci-ae',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '3761' & CATALOG_NBR <= '3761' ~ 'dn-a-csci-af',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '4034' & CATALOG_NBR <= '4034' ~ 'dn-a-csci-ag',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '4110' & CATALOG_NBR <= '4110' ~ 'dn-a-csci-ah',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '4287' & CATALOG_NBR <= '4287' ~ 'dn-a-csci-ai',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '4455' & CATALOG_NBR <= '4455' ~ 'dn-a-csci-aj',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '4551' & CATALOG_NBR <= '4551' ~ 'dn-a-csci-ak',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '4565' & CATALOG_NBR <= '4565' ~ 'dn-a-csci-al',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '4580' & CATALOG_NBR <= '4580' ~ 'dn-a-csci-am',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '4591' & CATALOG_NBR <= '4591' ~ 'dn-a-csci-an',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '4650' & CATALOG_NBR <= '4650' ~ 'dn-a-csci-ao',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '4738' & CATALOG_NBR <= '4739' ~ 'dn-a-csci-ap',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '4741' & CATALOG_NBR <= '4743' ~ 'dn-a-csci-aq',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '4930' & CATALOG_NBR <= '4931' ~ 'dn-a-csci-ar',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '4951' & CATALOG_NBR <= '4951' ~ 'dn-a-csci-as',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '2525' & CATALOG_NBR <= '2525' ~ 'dn-a-csci-at',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '1410' & CATALOG_NBR <= '1411' ~ 'dn-a-csci-s',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '1510' & CATALOG_NBR <= '1510' ~ 'dn-a-csci-t',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '2312' & CATALOG_NBR <= '2312' ~ 'dn-a-csci-u',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '2421' & CATALOG_NBR <= '2421' ~ 'dn-a-csci-v',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '2511' & CATALOG_NBR <= '2511' ~ 'dn-a-csci-w',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '3287' & CATALOG_NBR <= '3287' ~ 'dn-a-csci-y',
    campus == 'DN' & SBJCT_CD == 'CSCI' & CATALOG_NBR >= '3412' & CATALOG_NBR <= '3412' ~ 'dn-a-csci-z',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '3030' & CATALOG_NBR <= '3030' ~ 'dn-a-elec-ai',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '3215' & CATALOG_NBR <= '3215' ~ 'dn-a-elec-al',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '4025' & CATALOG_NBR <= '4025' ~ 'dn-a-elec-at',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '4133' & CATALOG_NBR <= '4133' ~ 'dn-a-elec-au',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '4134' & CATALOG_NBR <= '4134' ~ 'dn-a-elec-av',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '4184' & CATALOG_NBR <= '4184' ~ 'dn-a-elec-ay',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '4225' & CATALOG_NBR <= '4225' ~ 'dn-a-elec-az',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '4248' & CATALOG_NBR <= '4248' ~ 'dn-a-elec-ba',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '4276' & CATALOG_NBR <= '4276' ~ 'dn-a-elec-bb',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '4309' & CATALOG_NBR <= '4309' ~ 'dn-a-elec-bc',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '4319' & CATALOG_NBR <= '4319' ~ 'dn-a-elec-bd',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '4406' & CATALOG_NBR <= '4406' ~ 'dn-a-elec-bf',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '4423' & CATALOG_NBR <= '4423' ~ 'dn-a-elec-bg',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '4435' & CATALOG_NBR <= '4435' ~ 'dn-a-elec-bh',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '4444' & CATALOG_NBR <= '4444' ~ 'dn-a-elec-bi',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '4474' & CATALOG_NBR <= '4474' ~ 'dn-a-elec-bj',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '4637' & CATALOG_NBR <= '4637' ~ 'dn-a-elec-bk',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '4723' & CATALOG_NBR <= '4723' ~ 'dn-a-elec-bl',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '4727' & CATALOG_NBR <= '4727' ~ 'dn-a-elec-bm',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '5710' & CATALOG_NBR <= '5710' ~ 'dn-a-elec-bq',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '4174' & CATALOG_NBR <= '4174' ~ 'dn-a-elec-br',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '4555' & CATALOG_NBR <= '4555' ~ 'dn-a-elec-bs',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '5755' & CATALOG_NBR <= '5755' ~ 'dn-a-elec-bt',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '4005' & CATALOG_NBR <= '4005' ~ 'dn-a-elec-bu',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '5194' & CATALOG_NBR <= '5194' ~ 'dn-a-elec-bz',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '5294' & CATALOG_NBR <= '5294' ~ 'dn-a-elec-ca',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '4375' & CATALOG_NBR <= '4375' ~ 'dn-a-elec-cd',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '1520' & CATALOG_NBR <= '1520' ~ 'dn-a-elec-bv',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '2132' & CATALOG_NBR <= '2132' ~ 'dn-a-elec-cf',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '2142' & CATALOG_NBR <= '2142' ~ 'dn-a-elec-cg',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '2520' & CATALOG_NBR <= '2520' ~ 'dn-a-elec-ch',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '2531' & CATALOG_NBR <= '2531' ~ 'dn-a-elec-ci',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '3133' & CATALOG_NBR <= '3133' ~ 'dn-a-elec-cj',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '3225' & CATALOG_NBR <= '3225' ~ 'dn-a-elec-cl',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '3316' & CATALOG_NBR <= '3316' ~ 'dn-a-elec-cm',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '3520' & CATALOG_NBR <= '3520' ~ 'dn-a-elec-cn',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '3701' & CATALOG_NBR <= '3701' ~ 'dn-a-elec-co',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '3724' & CATALOG_NBR <= '3724' ~ 'dn-a-elec-cp',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '3817' & CATALOG_NBR <= '3817' ~ 'dn-a-elec-cv',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '3900' & CATALOG_NBR <= '3900' ~ 'dn-a-elec-cr',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '1510' & CATALOG_NBR <= '1510' ~ 'dn-a-elec-cs',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '2651' & CATALOG_NBR <= '2651' ~ 'dn-a-elec-ct',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '3164' & CATALOG_NBR <= '3164' ~ 'dn-a-elec-cu',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR %in% c('4164', '5164') ~ 'dn-a-elec-cw',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '4136' & CATALOG_NBR <= '4136' ~ 'dn-a-elec-cx',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR %in% c('4170', '5170') ~ 'dn-a-elec-cy',
    campus == 'DN' & SBJCT_CD == 'ELEC' & CATALOG_NBR >= '5446' & CATALOG_NBR <= '5446' ~ 'dn-a-elec-cz',
    campus == 'DN' & SBJCT_CD == 'ENGR' & CATALOG_NBR >= '3400' & CATALOG_NBR <= '3400' ~ 'dn-a-engr-a',
    campus == 'DN' & SBJCT_CD == 'ENGL' & CATALOG_NBR >= '1020' & CATALOG_NBR <= '1020' ~ 'dn-c-engl-a',
    campus == 'DN' & SBJCT_CD == 'ENGL' & CATALOG_NBR >= '2030' & CATALOG_NBR <= '2030' ~ 'dn-c-engl-b',
    TRUE ~ '')) %>%
  mutate(attr2 = case_when(
    campus == 'BD' & SBJCT_CD == 'FILM' & CATALOG_NBR >= '1000' & CATALOG_NBR <= '4999' & crseSec_comp_cd %in% c('LEC', 'SEM', 'WKS') ~ 'bd-c-film-a',
    campus == 'BD' & SBJCT_CD == 'IPHY' & CATALOG_NBR >= '3060' & CATALOG_NBR <= '3060' & crseSec_comp_cd %in% c('LAB') ~ 'bd-c-iphy-a',
    campus == 'BD' & SBJCT_CD == 'PHYS' & CATALOG_NBR >= '1020' & CATALOG_NBR <= '1020' & crseSec_comp_cd %in% c('LAB', 'REC') ~ 'bd-c-phys-a',
    campus == 'BD' & SBJCT_CD == 'PHYS' & CATALOG_NBR >= '1110' & CATALOG_NBR <= '1110' & crseSec_comp_cd %in% c('REC') ~ 'bd-c-phys-b, bd-c-phys-g',
    campus == 'BD' & SBJCT_CD == 'PHYS' & CATALOG_NBR >= '1120' & CATALOG_NBR <= '1120' & crseSec_comp_cd %in% c('REC') ~ 'bd-c-phys-c,bd-c-phys-h',
    campus == 'BD' & SBJCT_CD == 'PHYS' & CATALOG_NBR >= '1140' & CATALOG_NBR <= '1140' & crseSec_comp_cd %in% c('LAB', 'REC') ~ 'bd-c-phys-d',
    campus == 'BD' & SBJCT_CD == 'PHYS' & CATALOG_NBR >= '2010' & CATALOG_NBR <= '2010' & crseSec_comp_cd %in% c('LAB', 'REC') ~ 'bd-c-phys-e',
    campus == 'BD' & SBJCT_CD == 'PHYS' & CATALOG_NBR >= '2020' & CATALOG_NBR <= '2020' & crseSec_comp_cd %in% c('LAB', 'REC') ~ 'bd-c-phys-f',
    campus == 'BD' & SBJCT_CD == 'PHYS' & CATALOG_NBR >= '1115' & CATALOG_NBR <= '1115' & crseSec_comp_cd %in% c('REC') ~ 'bd-c-phys-i',
    campus == 'BD' & SBJCT_CD == 'PHYS' & CATALOG_NBR >= '1125' & CATALOG_NBR <= '1125' & crseSec_comp_cd %in% c('REC') ~ 'bd-c-phys-j',
    campus == 'DN' & SBJCT_CD == 'PUAD' & CATALOG_NBR >= '5001' & CATALOG_NBR <= '5001' & crseSec_comp_cd %in% c('LEC', 'SEM') ~ 'dn-a-puad-a',
    campus == 'DN' & SBJCT_CD == 'PUAD' & CATALOG_NBR >= '5002' & CATALOG_NBR <= '5002' & crseSec_comp_cd %in% c('LEC') ~ 'dn-a-puad-b',
    campus == 'DN' & SBJCT_CD == 'PUAD' & CATALOG_NBR >= '5003' & CATALOG_NBR <= '5003' & crseSec_comp_cd %in% c('LEC', 'SEM') ~ 'dn-a-puad-c',
    campus == 'DN' & SBJCT_CD == 'PUAD' & CATALOG_NBR >= '5004' & CATALOG_NBR <= '5004' & crseSec_comp_cd %in% c('LEC', 'SEM') ~ 'dn-a-puad-d',
    TRUE ~ '')) %>%
    mutate(attr3 = case_when(
    campus == 'BD' & fcqdept == 'ASEN' ~ 'bd-d-asen-c',
    campus == 'BD' & fcqdept == 'ATLS' ~ 'bd-d-atls-a',
    campus == 'BD' & fcqdept == 'CHEN' ~ 'bd-d-chen-b',
    campus == 'BD' & fcqdept == 'COMR' ~ 'bd-d-comr-a',
    campus == 'BD' & fcqdept == 'ECON' ~ 'bd-d-econ-a,bd-d-econ-c',
    campus == 'BD' & fcqdept == 'FYSM' ~ 'bd-d-fysm-a',
    campus == 'BD' & fcqdept == 'GEEN' ~ 'bd-d-geen-a',
    campus == 'BD' & fcqdept == 'HIST' ~ 'bd-d-hist-d',
    campus == 'BD' & fcqdept == 'HRAP' ~ 'bd-d-hrap-a',
    campus == 'BD' & fcqdept == 'ENES' ~ 'bd-d-huen-c',
    campus == 'BD' & fcqdept == 'LW' ~ 'bd-d-laws-a',
    campus == 'BD' & fcqdept == 'MASP' ~ 'bd-d-masp-a',
    campus == 'BD' & fcqdept == 'PHYS' ~ 'bd-d-phys-a',
    campus == 'BD' & fcqdept == 'PSYC' ~ 'bd-d-psyc-a,bd-d-psyc-b',
    campus == 'BD' & fcqdept == 'SASC' ~ 'bd-d-sasc-a,bd-d-sasc-c,bd-d-sasc-d,bd-d-sasc-e',
    campus == 'BD' & fcqdept == 'SLHS' ~ 'bd-d-slhs-a',
    campus == 'CE' & fcqdept == 'CC' ~ 'ce-d-cc-a',
    campus == 'CE' & fcqdept == 'CONT' ~ 'ce-d-cont-a',
    campus == 'DN' & fcqdept == 'BIOL' ~ 'dn-d-biol-c',
    campus == 'DN' & fcqdept == 'CVEN' ~ 'dn-d-cven-a',
    campus == 'DN' & fcqdept == 'EDUC' ~ 'dn-d-educ-a',
    campus == 'DN' & fcqdept == 'GEOG' ~ 'dn-d-geog-a',
    campus == 'DN' & fcqdept == 'GEOL' ~ 'dn-d-geol-a',
    campus == 'DN' & fcqdept == 'PHIL' ~ 'dn-d-phil-a',
    TRUE ~ '')) %>%
    mutate(attr4 = case_when(
    campus == 'BD' & fcqdept == 'CHEN' & crseSec_comp_cd %in% c('LEC', 'LAB', 'REC') ~ 'bd-d-chen-a',
    campus == 'BD' & fcqdept == 'ECON' & crseSec_comp_cd %in% c('LEC', 'SEM') ~ 'bd-d-econ-b',
    campus == 'BD' & fcqdept == 'GEOG' & crseSec_comp_cd %in% c('LAB', 'REC') ~ 'bd-d-geog-a',
    campus == 'BD' & fcqdept == 'NAVR' & crseSec_comp_cd %in% c('LEC') ~ 'bd-d-navr-a',
    campus == 'DN' & fcqdept == 'BIOE' & crseSec_comp_cd %in% c('LEC') ~ 'dn-d-bioe-a',
    campus == 'DN' & fcqdept == 'CMMU' & crseSec_comp_cd %in% c('LEC', 'REC', 'SEM') ~ 'dn-d-comm-a',
    TRUE ~ ''))

clscu3 <- clscu2 %>%
  mutate(sect_attr = case_when(
    attr1 != '' & attr2 == '' & attr3 == '' & attr4 == '' ~ attr1,
    attr1 != '' & attr2 != '' & attr3 == '' & attr4 == '' ~ paste(attr1, attr2, sep = ','),
    attr1 != '' & attr2 != '' & attr3 != '' & attr4 == '' ~ paste(attr1, attr2, attr3, sep = ','),
    attr1 != '' & attr2 != '' & attr3 != '' & attr4 != '' ~ paste(attr1, attr2, attr3, attr4, sep = ','),
    attr1 == '' & attr2 != '' & attr3 == '' & attr4 == '' ~ attr2,
    attr1 == '' & attr2 != '' & attr3 != '' & attr4 == '' ~ paste(attr2, attr3, sep = ','),
    attr1 == '' & attr2 != '' & attr3 != '' & attr4 != '' ~ paste(attr2, attr3, attr4, sep = ','),
    attr1 == '' & attr2 == '' & attr3 != '' & attr4 == '' ~ attr3,
    attr1 == '' & attr2 == '' & attr3 != '' & attr4 != '' ~ paste(attr3, attr4, sep = ','),
    attr1 == '' & attr2 == '' & attr3 == '' & attr4 != '' ~ attr4,
    TRUE ~ '' )) %>%
  select(-c(attr1, attr2, attr3, attr4))

# print
write.csv(clscu3, 'L:\\mgt\\FCQ\\CourseAudit\\clscu_r.csv', row.names = FALSE)
