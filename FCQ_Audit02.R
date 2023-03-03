#########################################################################
# Second stage of FCQ admin setup/course audit sequence
# L:\mgt\FCQ\R_Code\FCQ_Audit02.R - Vince Darcangelo, 9/19/22
#########################################################################

# TOC (order of operations)
# Part III: Create course audit columns and exceptions   (~ 3 mins)
# Part IV:  Set administration dates                     (fcq_audit03.r)
# Part V:   Generate audit files                         (fcq_audit04.r)

#########################################################################
#'*Part III: Create columns for audit*
#########################################################################

# indInstrNm
n01 <- clslistCU_3 %>%
  mutate(indInstrNm = case_when(
    SCHED_PRINT_INSTR == 'N' ~ 1,
    SCHED_PRINT_INSTR == '-' ~ 1,
    instrNm %in% c('-', '') ~ 1,
    TRUE ~ 0
  ))

# indGiveOnlyLEC
n01 <- n01 %>%
  mutate(indGiveOnlyLEC = case_when(
    deptOrgID %in% c('CUBLD:BLDR:GRMN', 'CUBLD:BLDR:EV') ~ 0,
    # BD-NRSC-5092 (2021/10/12 Amanda Meyer) REC sections of NRSC-5092 get FCQs
    deptOrgID == 'CUBLD:BLDR:PSYC' & SBJCT_CD == 'NRSC' & 
      CATALOG_NBR == '5092' ~ 0,
    # standard, no exceptions
    totAssocClassPerInstr >= 2 & totLECInstr == 1 & totNotLECInstr >= 1 
    & indNotLEC == 1 ~ 1,
    TRUE ~ 0
  ))

# indDIS_IND_THE and exceptions
n02 <- n01 %>%
  mutate(indDIS_IND_THE = case_when(
    crseSec_comp_cd %in% c('DIS', 'IND', 'THE') 
    # DN-PAFF & DN-EDUC-DSEP exception
    & paste(SBJCT_CD, CATALOG_NBR, sep = '-') != 'DSEP-8990' 
    & deptOrgID != 'CUDEN:PAFF:D-PAFF' ~ 1,
    # MC-CLN exception
    crseSec_comp_cd %in% c('CLN', 'RSC') & CAMPUS_CD == 'AMC' ~ 1,
    TRUE ~ 0
  )) 

# indDEPT_RQST
n03 <- n02 %>%
  mutate(indDEPT_RQST = case_when(
    # BD-IPHY 
    crseSec_comp_cd %in% c('INT', 'PRA') & deptOrgID == 'CUBLD:BLDR:IPHY' ~ 1,
    # BD-GEEN (2018/10/30 Ellen Parrish) FCQs for LEC ONLY, not LAB
    crseSec_comp_cd != 'LEC' & deptOrgID == 'CUBLD:BLDR:GEEN' ~ 1,
    # BD-EV (2021/02/26 Caitlin Anderson) courses get FCQs for LAB ONLY
    deptOrgID == 'CUBLD:BLDR:EV' & indLEC == 1 & totNotLECInstr >= 1 ~ 1,
    # BD-MCEN (2020/03/23 Vanessa Dunn) senior design FCQs for LEC ONLY
    crseSec_comp_cd != 'LEC' & deptOrgID == 'CUBLD:BLDR:MCEN' & 
      CATALOG_NBR %in% c('4045', '4085') ~ 1,
    # (2018/11/01 Deb Prestianni) no FCQs for 7116 and 7939
    deptOrgID == 'CUBLD:BLDR:LW' & CATALOG_NBR %in% c('7116', '7939') ~ 1,
    # DN-EDUC (2018/10/10 Lindsay Harn) no FCQs for 5v sections
    deptOrgID == 'CUDEN:EDUC:D-EDUC' & grepl('5v', CLASS_SECTION_CD, 
      ignore.case = TRUE) ~ 1,
    # MC-CLN 
    crseSec_comp_cd == 'CLN' & CAMPUS_CD == 'AMC' ~ 1,
    TRUE ~ 0
  )) 

# indIndptStdy
n04 <- n03 %>%
  mutate(indIndptStdy = case_when(
    # DN-PAFF and DN-COMM exceptions to independent study
    INSTRCTN_MODE_CD == 'IS'
    & !(deptOrgID %in% c('CUDEN:PAFF:D-PAFF', 'CUDEN:CLAS:D-COMM')) ~ 1,
    TRUE ~ 0
  ))

# indMinEnrl
n05 <- n04 %>%
  mutate(indMinEnrl = case_when(
    as.numeric(totEnrl_nowd_comb) <= 2 | 
      as.numeric(totEnrl_nowd) == 0 ~ 1,
    TRUE ~ 0
  ))

# indCandDegr
n06 <- n05 %>%
  mutate(indCandDegr = case_when(
    INSTRCTN_MODE_CD=='CN' & !(crseSec_comp_cd %in% c('IND', 'DIS')) ~ 1,
    TRUE ~ 0
  ))

# indNoInstr
n07 <- n06 %>%
  mutate(indNoInstr = case_when(
    totInstrPerClass == totSchedPrintInstr_Not_Y ~ 1,
    TRUE ~ 0
  ))

# indCombSect0
n08 <- n07 %>%
  mutate(indCombSect0 = case_when(
    totEnrl_nowd == 0 & combStat == 'S' ~ '.S',
    totEnrl_nowd == 0 & combStat == 'N' ~ '.N',
    TRUE ~ ''
  ))

# indCrseTooLate
# classes too late in term to get FCQs excluding Beijing classes
library('anytime')

n09 <- n08 %>%
  mutate(indCrseTooLate = case_when(
    campus == 'DN' & substr(CLASS_SECTION_CD,1,1) == 'B' ~ 0,
    substr(TERM_CD,4,4)=='1' & anydate(fcqEnDt) > anydate(semEndDt) ~ 1,
    substr(TERM_CD,4,4)=='4' & anydate(fcqEnDt) > anydate(semEndDt) ~ 1,
    substr(TERM_CD,4,4)=='7' & anydate(fcqEnDt) > anydate(semEndDt) ~ 1,
    TRUE ~ 0
  ))

# indEXSTD_MBA
n10 <- n09 %>%
  mutate(indEXSTD_MBA = case_when(
    CAMPUS_CD == 'EXSTD' & ACAD_GRP_CD == 'BUSN' & CLASS_SECTION_CD %in% c('533', '534') ~ 1,
    CAMPUS_CD == 'EXSTD' & ACAD_GRP_CD == 'BUSN' & SBJCT_CD %in% c('XHAD', 'GEMM') ~ 1,
    TRUE ~ 0
  ))

# cmbsecInfo
n11 <- n10 %>%
  mutate(cmbsecInfo = case_when(
    combStat == 'N' ~ paste('Enrlmt listed under', spons_id, sep = ' '),
    combStat == 'Y' ~ paste('Enrlmt includes', SCTN_CMBND_LD, sep = ' '),
    TRUE ~ ''
  ))

# indBeijing
# moved on 4/7/22 -- now managed in exceptions for FCQ_Audit03

# indCUSucceed
# not active as of 4/7/22 -- was for UCCS in SAS code

# indNo - add up 0/1 indicators for faster adminInd setup
n12 <- n11 %>%
  mutate(NoSum =
    rowSums(across(c(indInstrNm, indGiveOnlyLEC, indDIS_IND_THE, indDEPT_RQST, indIndptStdy, indMinEnrl, indCandDegr, indNoInstr, indCrseTooLate, indEXSTD_MBA)))) %>%
  mutate(indNo = case_when(
    NoSum >= 1 ~ 1,
    TRUE ~ 0
  ))

# adminInd (administration indicator) with exceptions
n13 <- n12 %>%
  mutate(adminInd = case_when(
    SCHED_PRINT_INSTR == 'N' ~ 0,
    SCHED_PRINT_INSTR == '-' ~ 0,
    indNo > 0 ~ 0,
    campus == 'BD' & SBJCT_CD == 'CSCV' ~ 0,
    fcqdept == 'EV' & indLEC == 1 & totNotLECInstr >= 1 ~ 0,
    fcqdept == 'GEEN' & crseSec_comp_cd != 'LEC' ~ 0,
    campus == 'CE' & SBJCT_CD == 'NCIE' ~ 0,
    campus == 'CE' & SESSION_CD == 'BM9' ~ 0,
  # (2020/07/08 Shawna Cox) MC-IDPT only run FCQs for 7806 and 7810
    SBJCT_CD == 'IDPT' & !(CATALOG_NBR %in% c('7806', '7810')) ~ 0,
    TRUE ~ 1
  ))

# fcqNote
n14 <- n13 %>%
  mutate(n01 = case_when(
    SCHED_PRINT_INSTR == 'N' ~ 'SCHED_PRINT_INSTR=NO')) %>%
  mutate(n02 = case_when(
    SCHED_PRINT_INSTR == '-' ~ 'SCHED_PRINT_INSTR=MISSING')) %>%
  mutate(n03 = case_when(
    INSTRCTN_MODE_CD == 'CN' & !(crseSec_comp_cd %in% c('IND', 'DIS')) 
      ~ 'NO FCQs for Cand Degr')) %>%
  mutate(n04 = case_when(
    instrNm == '' ~ 'No instrs showing for sctn')) %>%
  mutate(n05 = case_when(
    totAssocClassPerInstr >= 2 & totLECInstr == 1 & totNotLECInstr >= 2 
      & indNotLEC == 1
      ~ 'Instr gets FCQs for LEC only')) %>%
#################### exceptions
  # No FCQs for Anschutz CLN per Shawna Cox 7/1/20
  mutate(n06 = case_when(
    crseSec_comp_cd %in% c('CLN', 'RSC') & CAMPUS_CD == 'AMC' ~
      paste('No FCQs for', crseSec_comp_cd, 'classes', sep = ' '))) %>%
  # No INT/PRA FCQs for BD-IPHY per Marsha Cook 3/26/18
  mutate(n07 = case_when(
    crseSec_comp_cd %in% c('INT', 'PRA') & deptOrgID == 'CU:BLDR:IPHY' ~
      'No FCQs for INT/PRA classes')) %>%
  # No FCQs for CE-NCIE per Ruth Moore fall 2019
  mutate(n08 = case_when(
    campus=='CE' & SBJCT_CD == 'NCIE' ~ 'No FCQs for NCIE sections')) %>%
  # No FCQs for BD-CSVC
  mutate(n09 = case_when(
    campus=='BD' & SBJCT_CD == 'CSVC' ~ 'No FCQs for CSVC sections')) %>%
  # No FCQs for CE BM9 sections
  mutate(n10 = case_when(
    campus=='CE' & SESSION_CD == 'BM9' ~ 'No FCQs for BM9 sections')) %>%
  # BD-GEEN courses get FCQs for LEC only per Ellen Parrish 10/30/18
  mutate(n11 = case_when(
    deptOrgID == 'CUBLD:BLDR:GEEN' & crseSec_comp_cd != 'LEC' ~ 
      'No FCQs for non LEC sections')) %>%
  # BD-EV courses get FCQs for LAB only when there is LEC/LAB 
    # per Caitlin Anderson 2/26/21
  mutate(n12 = case_when(
    fcqdept == 'EV' & indLEC == 1 & totNotLECInstr >= 1 ~
      'FCQs administered for lab sections only')) %>%
  # BD-MCEN senior design classes (4045, 4085) get FCQs for LEC only 
    # per Vanessa Dunn 3/23/20
  mutate(n13 = case_when(
    deptOrgID == 'CUBLD:BLDR:MCEN' & CATALOG_NBR %in% c(4045, 4085) 
      & crseSec_comp_cd != 'LEC' ~ 'No FCQs for non LEC sections')) %>%
  # Only run two MC-IDPT courses per Shawna Cox 7/8/20
  mutate(n14 = case_when(
    SBJCT_CD == 'IDPT' & !(CATALOG_NBR %in% c(7806, 7810)) ~
      'NO FCQs for MEDSP sections of IDPT')) %>%
  # BD-LAWS no FCQs for 7116, 7939 classes per Deb Prestianni 11/1/18
  mutate(n15 = case_when(
    deptOrgID == 'CUBLD:BLDR:LW' & CATALOG_NBR %in% c(7116, 7939) ~
      'No FCQs for 7116, 7939 sections')) %>%
  # DN-EDUC no FCQs for 5V# courses per Lindsay Harn 10/10/18
  mutate(n16 = case_when(
    deptOrgID == 'CUDEN:EDUC:D-EDUC' & 
      grepl('5v', CLASS_SECTION_CD, ignore.case = TRUE) ~ 
      'No FCQs for 5V# sections')) %>%
  # No FCQs for IND, DIS, with exceptions
  mutate(n17 = case_when(
    INSTRCTN_MODE_CD == 'IS'
      & !(deptOrgID %in% c('CUDEN:PAFF:D-PAFF', 'CUDEN:CLAS:D-COMM')) ~
      'No FCQs for Indpt Study')) %>%
  # No FCQs for Cand Degr classes
  mutate(n18 = case_when(
    INSTRCTN_MODE_CD == 'CN' & !(crseSec_comp_cd %in% c('IND', 'DIS')) ~
      'No FCQs for Cand Degr')) %>%
  # Combined sections with no enrollment - review
  mutate(n19 = case_when(
    indCombSect0 == '.S' ~ 'No enrlmt but part of comb sctn with enrlmt')) %>%
  mutate(n20 = case_when(
    indCombSect0 == '.N' ~ 'comb sctn has no enrlmt')) %>%
  # Class ends too late for FCQs
  mutate(n21 = case_when(
    indCrseTooLate == 1 ~ 'Class ends too late for FCQs')) %>%
  # DN-BUSN classes not in FCQ program
  mutate(n22 = case_when(
    indEXSTD_MBA == 1 & CLASS_SECTION_CD %in% c(533, 534) ~ 
      'Accel MBA classes not in FCQ pgm')) %>%
  mutate(n23 = case_when(
      indEXSTD_MBA == 1 & SBJCT_CD == 'XHAD' ~
      'Exec MBA classes not in FCQ pgm')) %>%
  mutate(n24 = case_when(
        indEXSTD_MBA == 1 & SBJCT_CD == 'GEMM' ~
      'GEMM classes not in FCQ pgm')) %>%
  # totEnrl < 3
  mutate(n25 = case_when(
    indMinEnrl == 1 ~ 'Enrollment less than minimum')) %>%
  mutate(n26 = case_when(
    indGiveOnlyLEC == 1 ~ 'FCQs for LEC only')) %>%
  mutate(n27 = case_when(
    indDIS_IND_THE == 1 ~ 'No FCQs for DIS, IND, THE sections')) %>%
  # indDEPT_RQST
  mutate(n28 = case_when(
    indDEPT_RQST == 1 ~ 'No FCQs per department request'
  ))

# create fcqNote
n14$fcqNote <-apply(n14[,85:ncol(n14)],1,function(x) toString(na.omit(x)))

n14 <- n14 %>%
  mutate(fcqNote = case_when(
    adminInd == 1 & fcqNote != '' ~ '',
    TRUE ~ fcqNote))

#XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX#
# quality check
n14x <- n14 %>%
  filter(adminInd == 1 & fcqNote != '')
#XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX#
