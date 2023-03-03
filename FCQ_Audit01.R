#########################################################################
# First stage of FCQ admin setup/course audit sequence
# L:\mgt\FCQ\R_Code\FCQ_Audit01.R - Vince Darcangelo, 11/05/21
#########################################################################

# TOC (order of operations)
# Intro:    Run ciwpass.R to load ciw credentials        (ciwpass.r)
# Prologue: Update semester variables and load libraries (~ 2 mins)
# Part I:   Connect and pull data from ciw               (~ 7 mins)
# Part II:  Format data and prep for course audits       (< 1 mins)
# Part III: Create course audit columns and exceptions   (fcq_audit02.r)
# Part IV:  Set up administration dates                  (fcq_audit03.r)
# Part V:   Generate audit files                         (fcq_audit04.r)

#########################################################################
#'*Intro: Run ciwpass.R to load ciw credentials*
#########################################################################

#########################################################################
#'*Prologue: Update semester variables and load libraries*
#########################################################################

# update each semester
term_cd <- '2231'
minStuEnrl <- 3
setwd('L:\\mgt\\FCQ\\CourseAudit')

# semEndDt = spring: 05/31/YYYY, summer: 08/31/YYYY, fall: 12/31/YYYY
semEndDt <- as.Date('05/31/2023', format = '%m/%d/%Y')
semEndDt <- format(semEndDt, '%m/%d/%Y')

# load libraries
library('DBI')
library('ROracle')
library('tidyverse')
library('sqldf')
library('data.table')
library('lubridate')
library('formattable')

#########################################################################
#'*Part I: Connect and pull data from ciw*
#########################################################################

# connect to Oracle database
drv <- dbDriver('Oracle')
connection_string <- '(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)
  (HOST=ciw.prod.cu.edu)(PORT=1525))(CONNECT_DATA=(SERVICE_NAME=CIW)))'
con <- dbConnect(drv, username = getOption('databaseuid'), 
  password = getOption('databasepword'), dbname = connection_string)

# pull from PS_D_CLASS as c (~15 secs)
classtbl <- dbGetQuery(con,
  'SELECT CLASS_SID, TERM_CD, INSTITUTION_CD, CAMPUS_CD, SESSION_CD, ACAD_GRP_CD, ACAD_GRP_LD, ACAD_ORG_CD, ACAD_ORG_LD, SBJCT_CD, SBJCT_LD, CATALOG_NBR, CLASS_SECTION_CD, CLASS_NUM, CRSE_CD, CRSE_OFFER_NUM, ASSOCIATED_CLASS, GRADE_BASIS_CD, CLASS_STAT, CLASS_TYPE, ENRL_TOT, ENRL_CAP, ROOM_CAP_REQUEST, CRSE_LD, LOC_ID, SSR_COMP_CD, SSR_COMP_SD, INSTRCTN_MODE_CD, INSTRCTN_MODE_LD, COMBINED_SECTION, CLASS_START_DT, CLASS_END_DT, DATA_ORIGIN
  FROM PS_D_CLASS'
)

c <- classtbl %>%
  filter(TERM_CD == term_cd &
    INSTITUTION_CD != 'CUSPG' & 
    DATA_ORIGIN == 'S' &
    CLASS_STAT != 'X') %>%
  select(-DATA_ORIGIN)

# exclusions
cx <- c %>%
# limit to 4-digit catalog numbers: 
  # NCLL, NCEG exceptions per Kadie Goodman, Joanne Addison
  filter(nchar(CATALOG_NBR) == 4 | SBJCT_CD %in% c('NCLL', 'NCEG')) %>%
  filter(!(ACAD_GRP_CD %in% c('ZOTH', 'CONC', 'DENT')) &
    !(SBJCT_CD %in% c('STDY', 'STY', 'CAND')) &
    !(SBJCT_CD %in% c('CLDR', 'DTSA')) &
    !(SBJCT_CD %in% c('PHSL', 'MEDS', 'PRMD', 'PHMD', 'PALC', 'PHSC', 'TXCL')) &
# exclude org codes per requests from AMC and Boulder Registrar courses
    ACAD_ORG_CD != 'B-REGR' &
    !(ACAD_ORG_CD %in% c('D-DDSC', 'D-ANES', 'D-BIOS', 'D-CBHS', 'D-DERM', 'D-PHTR', 'D-PCSU', 'D-CCOH', 'D-EHOH', 'D-EMED', 'D-EPID', 'D-FMMD', 'D-NEUR', 'D-OBGY', 'D-OPHT', 'D-ORTH', 'D-OTOL', 'D-PHAS', 'D-PSCH', 'D-PATH', 'D-SURG', 'D-PHRD', 'D-HSMP', 'D-RAON', 'D-RADI', 'D-PEDS', 'D-NSUR', 'D-MEDI', 'D-PHNT', 'D-PUBH', 'D-PUNC', 'D-NURS'))) %>%
  filter(!(SBJCT_CD == 'IDPT' & !(CATALOG_NBR %in% c('7806', '7810')))) %>%
# exclude by campus: CEPS applied music and AMC 'RSC' courses
  filter(!(CAMPUS_CD == 'CEPS' & SESSION_CD == 'BM9') &
    !(CAMPUS_CD == 'AMC' & SSR_COMP_CD == 'RSC') &
    !(ACAD_GRP_CD == 'NOCR' & SBJCT_CD != 'NCLL') &
# exclude by institution/subject_cd: DN-MILR, BD-CSVC, MC-CLSC, MC-MSTP
    !(INSTITUTION_CD == 'CUBLD' & SBJCT_CD == 'CSVC') &
    !(INSTITUTION_CD == 'CUDEN' & SBJCT_CD == 'MILR') &
    !(INSTITUTION_CD == 'CUDEN' & SBJCT_CD == 'CLSC') &
    !(INSTITUTION_CD == 'CUDEN' & SBJCT_CD == 'MSTP') &
# exclude classes that dept didn't cancel correctly
    CLASS_NUM != 0 &
# exclude DN-MATH Gxx and 501 sections
    !(INSTITUTION_CD == 'CUDEN' & SBJCT_CD == 'MATH' & str_detect(CLASS_SECTION_CD, '^G')) &
    !(INSTITUTION_CD == 'CUDEN' & SBJCT_CD == 'MATH' & CLASS_SECTION_CD == '501'))

# pull from PS_F_CLASS_MTG_PAT as cmtg (~15 secs)
mtgtbl <- dbGetQuery(con,
  'SELECT TERM_CD, CLASS_NUM, CLASS_SECTION_CD, SCHED_PRINT_INSTR, PERSON_SID, INSTRCTR_ROLE_SID, MEETING_TIME_START, MEETING_TIME_END, START_DT, END_DT
  FROM PS_F_CLASS_MTG_PAT'
)

cmtg <- mtgtbl %>%
  filter(TERM_CD == term_cd &
    PERSON_SID != 2147483646) %>%
  mutate(icpair = paste0(CLASS_NUM, '_', PERSON_SID)) %>%
  mutate(icpair2 = paste0(CLASS_NUM, '_', PERSON_SID, '_', SCHED_PRINT_INSTR)) %>%
  select(-c(TERM_CD))

#########################################################################
# SEQUENCE: REDUCE MULTIPLE MTG PATTERN and DUPS TO SINGLE ROW

# identify/isolate single values (crse/instr pairings with 1 mtg or exact dups)
cmtg2a <- cmtg %>%
  group_by(icpair) %>%
  distinct() %>%
  filter(n() == 1)

# identify/isolate duplicate values (distinct dups)
cmtg2b <- cmtg %>%
  group_by(icpair) %>%
  distinct() %>%
  filter(n() >= 2)

# separate all dups with Y and collapse to single row
cmtg2bxy <- cmtg2b %>%
  filter(grepl('Y$', icpair2)) %>%
  filter(END_DT == max(END_DT))

# separate all dups with N and collapse to single row
cmtg2bxn <- cmtg2b %>%
  filter(grepl('N$', icpair2)) %>%
  filter(END_DT == max(END_DT))

# recombine the Y/N dups that have been reduced to singles
cmtg2b1 <- rbind(cmtg2bxy, cmtg2bxn)

# identify/isolate dups with single Y/N value
cmtg_assist <- cmtg2b1 %>%
  select(CLASS_NUM, SCHED_PRINT_INSTR, icpair, icpair2) %>%
  group_by(icpair2) %>%
  distinct() %>%
  group_by(icpair) %>%
  filter(n() == 1)

# identify/isolate dups with multiple Y/N value, reduce to Y value
cmtg_assist2 <- cmtg2b1 %>%
  select(CLASS_NUM, SCHED_PRINT_INSTR, icpair, icpair2) %>%
  group_by(icpair2) %>%
  distinct() %>%
  group_by(icpair) %>%
  filter(n() >= 2) %>%
  filter(SCHED_PRINT_INSTR == 'Y')

# recombine the single and reduced Y/N values
cmtg_assist3 <- rbind(cmtg_assist, cmtg_assist2)

# reconnect with cmtg2b (dups file)
cmtg2c <- cmtg2b1 %>%
  select(-c(CLASS_NUM, SCHED_PRINT_INSTR, icpair2)) %>%
  left_join(cmtg_assist3, by = 'icpair') %>%
  select(c(CLASS_NUM, CLASS_SECTION_CD, SCHED_PRINT_INSTR, PERSON_SID, INSTRCTR_ROLE_SID, START_DT, END_DT, icpair, icpair2)) %>%
  distinct() %>%
  filter(END_DT == max(END_DT)) %>%
  filter(INSTRCTR_ROLE_SID == max(INSTRCTR_ROLE_SID)) %>%
  filter(START_DT == min(START_DT))

cmtg2a <- cmtg2a %>% select(-c(MEETING_TIME_END, MEETING_TIME_START))

# recombine filtered cmtg data
cmtg3 <- rbind(cmtg2a, cmtg2c)

#########################################################################
# SEQUENCE: ACCOUNT FOR INSTRS NOT INCLUDED IN EVERY MTG PATTERN
# this occurs when a class has multiple mtg patterns, but an instr isn't listed in all of them -- particularly if they are only in the first mtg

# identify mismatched end dates
cmx1 <- cmtg3 %>%
  ungroup() %>%
  select(CLASS_NUM, START_DT, END_DT) %>%
  group_by(CLASS_NUM) %>%
  distinct() %>%
  filter(n() >= 2) %>%
  filter(END_DT == max(END_DT))

# isolate rows without this issue
cmx2 <- cmtg3 %>%
  anti_join(cmx1, by = 'CLASS_NUM')

# match rows with this issue, replacing START_DT/END_DT with max() values
cmx3 <- cmtg3 %>%
  select(-START_DT, -END_DT) %>%
  inner_join(cmx1, by = 'CLASS_NUM') %>%
  relocate(START_DT, .after = 'INSTRCTR_ROLE_SID') %>%
  relocate(END_DT, .before = 'icpair')

# recombine filtered cmtg data
cmtg4 <- rbind(cmx2, cmx3)

#XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX#
# quality check to remove dups, should == 0
cmtg4x <- cmtg4 %>%
  group_by(icpair2) %>%
  filter(n() >= 2)
#XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX#

# join cx & cmtg dataframes
j1 <- left_join(cx, cmtg4, by = 'CLASS_NUM')

# pull from PS_D_INSTRCTR_ROLE as irole (~1 sec)
irole <- dbGetQuery(con,
  'SELECT INSTRCTR_ROLE_SID, INSTRCTR_ROLE_CD
    FROM PS_D_INSTRCTR_ROLE'
)

j2 <- left_join(j1, irole, by = 'INSTRCTR_ROLE_SID')

####################################
# exp: pull from PS_CU_D_NAMES_II as ? (~ 1 min)
exp <- dbGetQuery(con,
  'SELECT EMPLID, LAST_NAME, FIRST_NAME, NAME_DISPLAY, LASTUPDDTTM, CURRENT_IND
  FROM SYSADM.PS_CU_D_NAMES_II'
)

colnames(exp) <- c('instrPersonID', 'instrLastNm', 'instrFirstNm', 'instrNm_src', 'LASTUPDDTTM', 'CURRENT_IND')

# PERSON_SID, EMPLID, NAME_TYPE, NAME, LAST_NAME, FIRST_NAME, SECOND_LAST_NAME, PREF_FIRST_NAME, NAME_DISPLAY, CURRENT_IND
####################################

exp2 <- exp %>%
  group_by(instrPersonID) %>%
  filter(CURRENT_IND == 'Y') %>%
  filter(LASTUPDDTTM == max(LASTUPDDTTM))

# Pull from PS_D_PERSON as pers (~1 min)
perstbl <- dbGetQuery(con,
  'SELECT PERSON_SID, PERSON_ID, PRF_PRI_NAME, PRF_PRI_LAST_NAME, PRF_PRI_FIRST_NAME, PRF_PRI_MIDDLE_NAME
  FROM PS_D_PERSON'
)

pers <- perstbl %>%
  filter(PERSON_SID != '2147483646')

colnames(pers) <- c('PERSON_SID', 'instrPersonID', 'instrNm_src', 'instrLastNm', 'instrFirstNm', 'instrMiddleNm')

############# name exceptions
# fix name anomalies
pers <- pers %>%
  mutate(instrNm_src = case_when(
    instrPersonID == '104724365' ~ 'Davis,Melissa',
    instrPersonID == '109554004' ~ 'Ngwu, Nancy',
#    instrPersonID == '110000649' ~ 'Miller,Laura',
    TRUE ~ instrNm_src)) %>%
  mutate(instrLastNm = case_when(
    instrPersonID == '104724365' ~ 'Davis',
    instrPersonID == '109554004' ~ 'Ngwu',
    TRUE ~ instrLastNm)) 
# %>%
#   mutate(instrFirstNm = case_when(
#     instrPersonID == '110000649' ~ 'Miller,Laura',
#     TRUE ~ instrFirstNm))
#############

j3 <- left_join(j2, pers, by = 'PERSON_SID')

# pull from PS_D_PERSON_EMAIL as em (~20 secs)
em <- dbGetQuery(con,
  'SELECT PERSON_ID, PREF_EMAIL, BLD_EMAIL, CONT_ED_EMAIL, DEN_EMAIL
  FROM PS_D_PERSON_EMAIL'
)

colnames(em) <- c('instrPersonID', 'PREF_EMAIL', 'BLD_EMAIL', 'CONT_ED_EMAIL', 'DEN_EMAIL')

j4 <- left_join(j3, em, by = 'instrPersonID')

# pull from PS_D_PERSON_ATTR as cid (~30 secs)
cid <- dbGetQuery(con, 
  'SELECT PERSON_ID, CONSTITUENT_ID
  FROM PS_D_PERSON_ATTR'
)

colnames(cid) <- c('instrPersonID', 'instrConstituentID')

j5 <- left_join(j4, cid, by = 'instrPersonID')

# pull from PS_CU_D_EXT_SYSTEM as hr (~12 secs)
hrtbl <- dbGetQuery(con,
  'SELECT EXTERNAL_SYSTEM, PERSON_SID, EXTERNAL_SYSTEM_ID
  FROM PS_CU_D_EXT_SYSTEM'
)

hr0 <- hrtbl %>%
  filter(EXTERNAL_SYSTEM == 'HR' & PERSON_SID != '2147483646') %>%
  select(-EXTERNAL_SYSTEM)

hr <- unique(hr0)
colnames(hr) <- c('PERSON_SID', 'instrEmplid')

j6 <- left_join(j5, hr, by = 'PERSON_SID')

# pull from PS_CU_D_CLASS_ATTR as cattr (~6 secs)
cattrtbl <- dbGetQuery(con,
  'SELECT CRSE_ATTR_CD, DATA_ORIGIN, CLASS_SID, CRSE_ATTR_VALUE_CD
  FROM PS_CU_D_CLASS_ATTR'
)

cattr <- cattrtbl %>%
  filter(CRSE_ATTR_CD == 'COMB' & DATA_ORIGIN == 'S') %>%
  select(CLASS_SID, CRSE_ATTR_VALUE_CD) %>%
  mutate(combStat = case_when(
    CRSE_ATTR_VALUE_CD == 'SPONSOR' ~ 'S',
    CRSE_ATTR_VALUE_CD == 'NON-SPONSR' ~ 'N',
    CRSE_ATTR_VALUE_CD == TRUE ~ '-'
))

j7 <- left_join(j6, cattr, by = 'CLASS_SID')

# pull from PS_CU_D_SCTN_CMBND as cmbsec (~1 sec)
sectbl <- dbGetQuery(con,
  'SELECT INSTITUTION_CD, TERM_CD, SESSION_CD, CLASS_NUM, SCTN_CMBND_CD, SCTN_CMBND_LD, SCTN_CMBND_EN_TOT, DATA_ORIGIN
  FROM PS_CU_D_SCTN_CMBND'
)

cmbsec <- sectbl %>%
  filter(TERM_CD == term_cd & DATA_ORIGIN == 'S') %>%
  select(-DATA_ORIGIN)

j8 <- left_join(j7, cmbsec, by = c('TERM_CD', 'INSTITUTION_CD', 'SESSION_CD', 'CLASS_NUM'))

# pull from PS_F_CLASS_ENRLMT as enr (~2 mins)
enrtbl <- dbGetQuery(con,
  'SELECT ENRLMT_STAT_SID, ENRLMT_DROP_DT_SID, DATA_ORIGIN, CLASS_SID
  FROM PS_F_CLASS_ENRLMT'
)

enr <- enrtbl %>%
  filter(ENRLMT_STAT_SID == '3' & ENRLMT_DROP_DT_SID == 19000101 
         & DATA_ORIGIN == 'S') %>%
  select(CLASS_SID)

enr <- enr %>% count(CLASS_SID)
colnames(enr) <- c('CLASS_SID', 'totEnrl')

j9 <- left_join(j8, enr, by = 'CLASS_SID')

#########################################################################
#'*Part II: Format data and prep for course audits'*
#########################################################################

# differentiate start/end date for ceps-hybrid courses
j9 <- j9 %>%
  mutate(mtgStartDt = case_when(
    INSTRCTN_MODE_CD %in% c('HY', 'H1', 'H2') ~ format(as.POSIXct(j9$START_DT, format='%Y:%m:%d %H:%M:%S'), '%m/%d/%Y'),
    TRUE ~ format(as.POSIXct(j9$START_DT, format='%Y:%m:%d %H:%M:%S'), '%m/%d/%Y')
  )) %>%
  mutate(mtgEndDt = case_when(
    INSTRCTN_MODE_CD %in% c('HY', 'H1', 'H2') ~ format(as.POSIXct(j9$CLASS_END_DT, format='%Y:%m:%d %H:%M:%S'), '%m/%d/%Y'),
    TRUE ~ format(as.POSIXct(j9$END_DT, format='%Y:%m:%d %H:%M:%S'), '%m/%d/%Y')
  ))

# separate, format and rejoin times from CLASS_START_DT and CLASS_END_DT
j9$cst <- format(as.POSIXct(j9$CLASS_START_DT, 
  format='%Y:%m:%d %H:%M:%S'), '%H:%M:%S')
j9$csd <- format(as.POSIXct(j9$CLASS_START_DT, 
  format='%Y:%m:%d %H:%M:%S'), '%d%b%y')
j9$cet <- format(as.POSIXct(j9$CLASS_END_DT, 
  format='%Y:%m:%d %H:%M:%S'), '%H:%M:%S')
j9$ced <- format(as.POSIXct(j9$CLASS_END_DT, 
  format='%Y:%m:%d %H:%M:%S'), '%d%b%y')
j9$fcqStDt <- j9$mtgStartDt
j9$fcqEnDt <- j9$mtgEndDt
j9$CLASS_START_DT <- toupper(paste(j9$csd, j9$cst, sep = ':'))
j9$CLASS_END_DT <- toupper(paste(j9$ced, j9$cet, sep = ':'))

# convert NA values to blanks
j9$combStat[is.na(j9$combStat)] <- ''
j9$SCTN_CMBND_CD[is.na(j9$SCTN_CMBND_CD)] <- ''
j9$SCTN_CMBND_LD[is.na(j9$SCTN_CMBND_LD)] <- ''
j9$instrNm_src[is.na(j9$instrNm_src)] <- ''
j9$instrFirstNm[is.na(j9$instrFirstNm)] <- ''
j9$instrLastNm[is.na(j9$instrLastNm)] <- ''
j9$instrMiddleNm[is.na(j9$instrMiddleNm)] <- ''
j9$instrEmplid[is.na(j9$instrEmplid)] <- ''

# convert NA values to -
j9$CLASS_SECTION_CD.x[is.na(j9$CLASS_SECTION_CD.x)] <- '-'
j9$SCHED_PRINT_INSTR[is.na(j9$SCHED_PRINT_INSTR)] <- '-'
j9$fcqStDt[is.na(j9$fcqStDt)] <- '-'
j9$fcqEnDt[is.na(j9$fcqEnDt)] <- '-'
j9$mtgStartDt[is.na(j9$mtgStartDt)] <- '-'
j9$mtgEndDt[is.na(j9$mtgEndDt)] <- '-'
j9$INSTRCTR_ROLE_CD[is.na(j9$INSTRCTR_ROLE_CD)] <- '-'
j9$instrPersonID[is.na(j9$instrPersonID)] <- '-'
j9$PREF_EMAIL[is.na(j9$PREF_EMAIL)] <- '-'
j9$BLD_EMAIL[is.na(j9$BLD_EMAIL)] <- '-'
j9$CONT_ED_EMAIL[is.na(j9$CONT_ED_EMAIL)] <- '-'
j9$DEN_EMAIL[is.na(j9$DEN_EMAIL)] <- '-'
j9$instrConstituentID[is.na(j9$instrConstituentID)] <- '-'
j9$SCTN_CMBND_EN_TOT[is.na(j9$SCTN_CMBND_EN_TOT)] <- '-'
j9$totEnrl[is.na(j9$totEnrl)] <- '-'

# update combined enrollment for non-combined classes
j9 <- j9 %>%
  mutate(totEnrl_nowd_comb = case_when(
    COMBINED_SECTION != '-' ~ SCTN_CMBND_EN_TOT,
    COMBINED_SECTION == '-' ~ totEnrl
  ))

# convert - values to 0 in enrollment columns
j9$totEnrl_nowd_comb <- gsub('-', 0, j9$totEnrl_nowd_comb)
j9$totEnrl <- gsub('-', 0, j9$totEnrl)

# add space after comma in instrNm_src col (e.g., 'Smith,J' -> 'Smith, J')
j9$instrNm_src <- gsub(',', ', ', j9$instrNm_src)

# create instrEmailAddr column based on email columns
j9 <- j9 %>%
  mutate(instrEmailAddr = case_when(
    INSTITUTION_CD == 'CUBLD' & BLD_EMAIL != '-' ~ BLD_EMAIL,
    CAMPUS_CD == 'CEPS' & CONT_ED_EMAIL != '-' ~ CONT_ED_EMAIL,
    CAMPUS_CD == 'CEPS' & BLD_EMAIL != '-' ~ BLD_EMAIL,
    INSTITUTION_CD == 'CUDEN' & DEN_EMAIL != '-' ~ DEN_EMAIL,
    PREF_EMAIL != '-' ~ PREF_EMAIL
  ))
j9$instrEmailAddr[is.na(j9$instrEmailAddr)] <- '-'

# drop unnecessary columns
clist = subset(j9, select = -c(BLD_EMAIL, ced, cet, CLASS_SECTION_CD.y, CLASS_SID, COMBINED_SECTION, CONT_ED_EMAIL, CRSE_ATTR_VALUE_CD, CRSE_CD, CRSE_OFFER_NUM, csd, cst, DEN_EMAIL, INSTRCTN_MODE_LD, INSTRCTR_ROLE_SID, PERSON_SID, PREF_EMAIL, SBJCT_LD, SCTN_CMBND_EN_TOT, SSR_COMP_SD))

# fix column names
names(clist)[names(clist) == 'CLASS_MTG_NUM'] <- 'minClassMtgNum'
names(clist)[names(clist) == 'CLASS_SECTION_CD.x'] <- 'CLASS_SECTION_CD'
names(clist)[names(clist) == 'instrNm_src'] <- 'instrNm'
names(clist)[names(clist) == 'totEnrl'] <- 'totEnrl_nowd'

# create campus column to generate two-digit campus code from CAMPUS_CD
cl0 <- clist %>%
  mutate(campus = case_when(
    INSTITUTION_CD == 'CUBLD' & CAMPUS_CD == 'BLD3' ~ 'B3',
    INSTITUTION_CD == 'CUBLD' & CAMPUS_CD == 'BLDR' ~ 'BD',
    INSTITUTION_CD == 'CUBLD' & CAMPUS_CD == 'CEPS' ~ 'CE',
    INSTITUTION_CD == 'CUDEN' & SBJCT_CD == 'CHPM' ~ 'MC',
    INSTITUTION_CD == 'CUDEN' & CAMPUS_CD == 'DC' ~ 'DN',
    INSTITUTION_CD == 'CUDEN' & CAMPUS_CD == 'EXSTD' ~ 'DN',
    INSTITUTION_CD == 'CUDEN' & CAMPUS_CD == 'AMC' ~ 'MC'
  ))

# create college column to generate two-digit college code from ACAD_GRP_CD
cl1 <- cl0 %>%
  mutate(college = case_when(
    campus == 'B3' & ACAD_GRP_CD == 'CRSS' ~ 'AS',
    campus == 'B3' & ACAD_GRP_CD == 'EDUC' ~ 'EB',
    campus == 'B3' & ACAD_GRP_CD == 'ENGR' ~ 'EN',
    campus == 'BD' & ACAD_GRP_CD == 'ARSC' ~ 'AS',
    campus == 'BD' & ACAD_GRP_CD == 'BUSN' ~ 'BU',
    campus == 'BD' & ACAD_GRP_CD == 'CRSS' ~ 'XY',
    campus == 'BD' & ACAD_GRP_CD == 'CMCI' ~ 'MC',
    campus == 'BD' & ACAD_GRP_CD == 'ARPL' ~ 'EV',
    campus == 'BD' & ACAD_GRP_CD == 'ENGR' ~ 'EN',
    campus == 'BD' & ACAD_GRP_CD == 'LIBR' ~ 'LB',
    campus == 'BD' & ACAD_GRP_CD == 'EDUC' ~ 'EB',
    campus == 'BD' & ACAD_GRP_CD == 'MUSC' ~ 'MB',
    campus == 'BD' & ACAD_GRP_CD == 'LAWS' ~ 'LW',
    campus == 'CE' ~ 'CE',
    campus == 'MC' ~ 'MC',
    campus == 'DN' & ACAD_GRP_CD == 'BUSN' ~ 'BD',
    campus == 'DN' & ACAD_GRP_CD == 'CLAS' ~ 'LA',
    campus == 'DN' & ACAD_GRP_CD == 'ARPL' ~ 'AP',
    campus == 'DN' & ACAD_GRP_CD == 'ARTM' ~ 'AM',
    campus == 'DN' & ACAD_GRP_CD == 'ENGR' ~ 'EN',
    campus == 'DN' & ACAD_GRP_CD == 'EDUC' ~ 'ED',
    campus == 'DN' & ACAD_GRP_CD == 'PAFF' ~ 'PA',
    campus == 'DN' & ACAD_GRP_CD == 'CRSS' ~ 'XY',
    campus == 'DN' & ACAD_GRP_CD == 'NOCR' ~ 'LA',
  ))

# Separate into RAP/non-RAP/CEPS sections
cl2 <- subset(cl1, !grepl('R$', CLASS_SECTION_CD))
cl2r <- subset(cl1, grepl('R$', CLASS_SECTION_CD))
cl2c <- subset(cl2, campus == 'CE')
cl2s <- subset(cl2, campus != 'CE')

# Non-RAP: Create keys to prep for fcqdept column
cl2s <- cl2s %>%
  mutate(key = case_when(
    campus == 'B3' ~ paste(campus, college, SBJCT_CD, sep = '-'),
    ACAD_ORG_CD == 'B-MCEN' ~ paste(campus, 'EN', SBJCT_CD, sep = '-'),
    campus == 'BD' ~ paste(campus, college, SBJCT_CD, sep = '-'),
    campus == 'CE' ~ paste(campus, SBJCT_CD, sep = '-'),
    ACAD_GRP_CD == 'MEDS' ~ paste('MC', 'MC', SBJCT_CD, sep = '-'),
    ACAD_GRP_CD == 'PHAR' ~ paste('MC', 'MC', SBJCT_CD, sep = '-'),
    campus == 'DN' ~ paste(campus, college, SBJCT_CD, sep = '-'),
  ))

# CEPS: Create numeric column for case_when range
cl2c <- cl2c %>%
  mutate(sec = substr(CLASS_SECTION_CD, 0, 2))

# CEPS: Sort by dept
cl2c2 <- cl2c %>%
  mutate(key = case_when(
    SBJCT_CD == 'ORGL' ~ 'CE-ORGL',
    SBJCT_CD == 'ESLG' | SBJCT_CD == 'NCIE' ~ 'CE-IEC',
    SESSION_CD == 'BEF' | SESSION_CD == 'BET' | SESSION_CD == 'BWS' ~ 'CE-CONT',
    sec >= 73 & sec <= 75 ~ 'CE-BBAC',
    sec >= 56 & sec <= 57 ~ 'CE-TRCT',
    sec >= 58 & sec <= 59 ~ 'CE-CC',
    sec >= 64 & sec <= 64 ~ 'CE-CC',
    TRUE ~ 'CE-CONT'
  ))

# RAP: Create numeric column for case_when range
cl2r <- cl2r %>%
  mutate(sec = substr(CLASS_SECTION_CD, 1, nchar(CLASS_SECTION_CD)-1))
cl2r <- transform(cl2r, sec = as.numeric(sec))

# RAP: Create keys to prep for fcqdept column
cl2r <- cl2r %>%
  mutate(key = case_when(
    sec >= 130 & sec < 160 ~ paste(campus, 'AS', 'GSAP', sep = '-'),
    sec >= 160 & sec < 190 ~ paste(campus, 'AS', 'SEWL', sep = '-'),
    sec >= 190 & sec < 220 ~ paste(campus, 'AS', 'HPRP', sep = '-'),
    sec >= 220 & sec < 250 ~ paste(campus, 'AS', 'COMR', sep = '-'),
    sec >= 250 & sec < 280 ~ paste(campus, 'AS', 'FARR', sep = '-'),
    sec >= 280 & sec < 310 ~ paste(campus, 'AS', 'LIBB', sep = '-'),
    sec >= 310 & sec < 340 ~ paste(campus, 'AS', 'SASC', sep = '-'),
    sec >= 370 & sec < 400 ~ paste(campus, 'AS', 'SSIR', sep = '-'),
    sec >= 400 & sec < 430 ~ paste(campus, 'AS', 'MASP', sep = '-'),
    sec >= 430 & sec < 460 ~ paste(campus, 'AS', 'BRAP', sep = '-'),
    sec >= 490 & sec < 520 ~ paste(campus, 'AS', 'VCAA', sep = '-'),
    sec >= 430 & sec < 460 ~ paste(campus, 'AS', 'BRAP', sep = '-'),
    sec == 549 ~ paste(campus, college, SBJCT_CD, sep = '-'),
    sec >= 550 & sec < 580 ~ paste(campus, 'AS', 'CUDC', sep = '-'),
    sec >= 610 & sec < 670 ~ paste(campus, 'BU', 'BU', sep = '-'),
    sec >= 888 & sec < 910 ~ paste(campus, 'AS', 'HRAP', sep = '-')
  ))

# subset RAPs with NA
cl2r <- subset(cl2r, !is.na(key))
cl2n <- subset(cl2r, is.na(key))

# replace key column for RAP with key formula for non-RAP
cl2n <- subset(cl2n, select = -key)
cl2n <- cl2n %>% mutate(key = paste(campus, CAMPUS_CD, SBJCT_CD, sep = '-'))

# remove sec columns
cl2c2 <- subset(cl2c2, select = -sec)
cl2n <- subset(cl2n, select = -sec)
cl2r <- subset(cl2r, select = -sec)

# merge all classes back together now that they all have key columns
clmerge <- rbind(cl2s, cl2c2, cl2r, cl2n)

# load FCQ department keys
dept_key <- read.csv('L:\\mgt\\FCQ\\R_Code\\FCQDept_keys.csv')

# remove dups
dept_key2 <- subset(dept_key, fcqdept != 'SBDR')
dept_key <- subset(dept_key2, !(campus == 'CE' & subject == 'EDUC' 
  & fcqdept == 'CONT'))

# merge tables to create fcqdept var
clmerge$fcqdept <- dept_key$fcqdept[match(clmerge$key, dept_key$key)]

#XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX#
# SEQUENCE: IDENTIFY NEW SUBJECTS NOT YET ASSIGNED AN FCQ DEPT

# identify new subjects
nna <- clmerge %>%
  filter(is.na(fcqdept))

# if nna == 0, no change required, continue to cl4

# if nna >= 1, use nna to identify, lookup subjects in CU-SIS, confirm acad group, add to dept_key doc, then rerun from load FCQ department keys
#XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX#

# create deptOrgID column
cl4 <- clmerge %>%
  mutate(deptOrgID = case_when(
    INSTITUTION_CD == 'CUBLD' ~ paste(INSTITUTION_CD, CAMPUS_CD, fcqdept, sep = ':'),
    TRUE ~ paste(INSTITUTION_CD, ACAD_GRP_CD, ACAD_ORG_CD, sep = ':')
  ))

# separate sponsor/blank from non-sponsor sections
spons <- subset(cl4, combStat != 'N')
nspons <- subset(cl4, combStat == 'N')

# create spons_id, _comp_cd, _deptOrgID, _AcadGrp, _AcadOrg, _fcqdept column
spons1 <- spons %>%
    mutate(spons_id = paste(SBJCT_CD, CATALOG_NBR, CLASS_SECTION_CD, sep = '-'))
spons2 <- spons1 %>%
  mutate(spons_comp_cd = SSR_COMP_CD)
spons3 <- spons2 %>%
  mutate(spons_deptOrgID = deptOrgID)
spons4 <- spons3 %>%
  mutate(spons_AcadGrp = ACAD_GRP_CD)
spons5 <- spons4 %>%
  mutate(spons_AcadOrg = ACAD_ORG_CD)
spons6 <- spons5 %>%
  mutate(spons_fcqdept = fcqdept)

# filter rows and columns to sponsor sections and data
spons7 <- subset(spons6, combStat == 'S')
spons8 <- spons7[,c('INSTITUTION_CD','SCTN_CMBND_CD','SCTN_CMBND_LD', 'instrNm','spons_id','spons_comp_cd','spons_deptOrgID','spons_AcadGrp', 'spons_AcadOrg', 'spons_fcqdept')]

# duplicate spons_comp_cd column to retain during join
spons8 <- spons8 %>% 
  mutate(spons_comp_cd2 = spons_comp_cd)

# merge spons columns onto nspons
nspons1 <- unique(nspons)
nspons2 <- subset(nspons1[!is.na(nspons1$SCTN_CMBND_CD),])
nspons3 <- left_join(nspons2, spons8, by = c('INSTITUTION_CD', 'SCTN_CMBND_CD', 'SCTN_CMBND_LD', 'instrNm'))
nspons3 <- subset(nspons3, select = -spons_comp_cd2)
nspons4 <- nspons3[!duplicated(nspons3[,c('CLASS_NUM', 'instrPersonID')]),]

# recombine spons and nspons data
clist2 <- rbind(spons6, nspons4)

# duplicate spons_id column as assoc_class_secID
clist3 <- clist2 %>% 
  mutate(assoc_class_secID = case_when(
    ASSOCIATED_CLASS == 9999 ~ '',
    TRUE ~ spons_id))

# fix column names
names(clist3)[names(clist3) == 'spons_comp_cd'] <- 'crseSec_comp_cd'
names(clist3)[names(clist3) == 'campus.x'] <- 'campus'

# update order to prep for instrNum
clist5 <- clist3[order(clist3$INSTITUTION_CD,clist3$SBJCT_CD,clist3$CATALOG_NBR,clist3$CLASS_SECTION_CD,clist3$INSTRCTR_ROLE_CD,clist3$instrNm),]

# create number sequence for instrNum
clist6 <- clist5$instrNum <- sequence(rle(clist5$CLASS_NUM)$lengths)
clist6 <- as.data.frame(clist6)

# combine into crslistCU
crslistCU <- cbind(clist5, clist6)

# fix column names
names(crslistCU)[names(crslistCU) == 'clist6'] <- 'instrNum'

# arrange columns
crslistCU <- crslistCU %>%
  select(campus,deptOrgID,fcqdept,TERM_CD,INSTITUTION_CD,CAMPUS_CD,SESSION_CD,SBJCT_CD,CATALOG_NBR,CLASS_SECTION_CD,instrNum,instrNm,instrLastNm,instrFirstNm,instrMiddleNm,instrPersonID,instrConstituentID,instrEmplid,instrEmailAddr,INSTRCTR_ROLE_CD,ASSOCIATED_CLASS,assoc_class_secID,GRADE_BASIS_CD,totEnrl_nowd_comb,totEnrl_nowd,ENRL_TOT, ENRL_CAP,ROOM_CAP_REQUEST,CRSE_LD,LOC_ID,CLASS_STAT,crseSec_comp_cd,SSR_COMP_CD,combStat,spons_AcadGrp,spons_AcadOrg,spons_fcqdept,spons_deptOrgID,spons_id,SCTN_CMBND_CD,SCTN_CMBND_LD,INSTRCTN_MODE_CD,CLASS_TYPE,SCHED_PRINT_INSTR,mtgStartDt,mtgEndDt,CLASS_START_DT,fcqStDt,fcqEnDt,CLASS_END_DT,CLASS_NUM,ACAD_GRP_CD,ACAD_GRP_LD,ACAD_ORG_CD,ACAD_ORG_LD)

# update order to match C10
crslistCU <- crslistCU[order(crslistCU$campus, crslistCU$deptOrgID),]

# filter crslistCU
clst_filter <- subset(crslistCU, totEnrl_nowd_comb != 0)

# create clslistCU_1 by grouping crslistCU
clslistCU_1 <- clst_filter %>%
  group_by(campus, fcqdept, SBJCT_CD, CATALOG_NBR, CLASS_SECTION_CD) %>%
  mutate(totSchedPrintInstr_Y = sum(SCHED_PRINT_INSTR == 'Y')) %>%
  mutate(totSchedPrintInstr_Not_Y = sum(SCHED_PRINT_INSTR %in% c('N', '-'))) %>%
  mutate(totInstrPerClass = n(), across()) %>%
  ungroup()

# order clslistCU_1
clslistCU_1[order(c('campus', 'deptOrgID', 'SBJCT_CD', 'CATALOG_NBR', 'CLASS_SECTION_CD'))]

# create clslistCU_2 by summarise() crslistCU_1
clslistCU_2 <- clslistCU_1 %>%
  group_by(campus, deptOrgID, SBJCT_CD, CATALOG_NBR, crseSec_comp_cd, instrNm) %>%
  summarise(totCrsePerInstr = sum(SCHED_PRINT_INSTR == 'Y'), across()) %>%
  ungroup()

# order clslistCU_2
clslistCU_2[order(c('campus', 'deptOrgID', 'SBJCT_CD', 'CATALOG_NBR', 'CLASS_SECTION_CD', 'instrNm'))]

# create clslistCU_3 by summarise() crslistCU_2
clslistCU_3 <- clslistCU_2 %>%
  group_by(SBJCT_CD, CATALOG_NBR, ASSOCIATED_CLASS, instrNm) %>%
  mutate(indLEC = case_when(
    instrNm != '-' & crseSec_comp_cd %in% c('LEC', 'SEM')
    & SCHED_PRINT_INSTR == 'Y' ~ 1,
    TRUE ~ 0)) %>%
  mutate(indNotLEC = case_when(
    instrNm != '-' & !(crseSec_comp_cd %in% c('LEC', 'SEM'))
    & SCHED_PRINT_INSTR == 'Y' ~ 1,
    TRUE ~ 0)) %>%
  mutate(totLECInstr = sum(indLEC)) %>%
  mutate(totNotLECInstr = sum(indNotLEC)) %>%
  mutate(nm_prep = case_when(
    instrNm != '-' ~ 1)) %>%
  mutate(totAssocClassPerInstr = sum(nm_prep))

# order clslistCU_3
clslistCU_3[order(c('campus', 'deptOrgID', 'SBJCT_CD', 'CATALOG_NBR', 'CLASS_SECTION_CD', rev('SCHED_PRINT_INSTR'), 'instrNm'))]

# arrange columns
clslistCU_3 <- clslistCU_3 %>%
  select(campus,deptOrgID,fcqdept,TERM_CD,INSTITUTION_CD,CAMPUS_CD,SESSION_CD,SBJCT_CD,CATALOG_NBR,CLASS_SECTION_CD,instrNum,instrNm,instrLastNm,instrFirstNm,instrMiddleNm,instrPersonID,instrConstituentID, instrEmplid,instrEmailAddr,INSTRCTR_ROLE_CD,ASSOCIATED_CLASS,assoc_class_secID,GRADE_BASIS_CD,totEnrl_nowd_comb,totEnrl_nowd,ENRL_TOT,ENRL_CAP,ROOM_CAP_REQUEST,CRSE_LD,LOC_ID,CLASS_STAT,crseSec_comp_cd,SSR_COMP_CD,combStat,spons_AcadGrp,spons_AcadOrg,spons_fcqdept,spons_deptOrgID,spons_id,SCTN_CMBND_CD,SCTN_CMBND_LD,INSTRCTN_MODE_CD,CLASS_TYPE,SCHED_PRINT_INSTR,mtgStartDt,mtgEndDt,CLASS_START_DT,CLASS_END_DT,CLASS_NUM,ACAD_GRP_CD,ACAD_GRP_LD,ACAD_ORG_CD,ACAD_ORG_LD,totSchedPrintInstr_Y,totSchedPrintInstr_Not_Y,totInstrPerClass,totCrsePerInstr,totAssocClassPerInstr,indLEC,indNotLEC,totLECInstr,totNotLECInstr,fcqStDt,fcqEnDt)

# export files to L:\\mgt\\FCQ\\CourseAudit directory
write.csv(clslistCU_3, paste0('L:\\mgt\\FCQ\\CourseAudit\\', term_cd, '\\c20.csv'), row.names = FALSE)
##########################################################
dbDisconnect(con)