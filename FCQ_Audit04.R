#########################################################################
# Fourth stage of FCQ admin setup/course audit sequence
# L:\mgt\FCQ\R_Code\course_audit\FCQ_Audit04.R - Vince Darcangelo, 9/19/22
#########################################################################

# TOC (order of operations)
# Part V:   Generate audit files                         (~ 30 secs)

#########################################################################
#'*Part V: Generate audit files*
#########################################################################

library('openxlsx')

# format deptOrgID
clscu$deptOrgID <- gsub(':','_',clscu$deptOrgID)

# create auditprep dataset
auditprep <- clscu %>%
  ungroup() %>%
  select(adminInd, fcqNote, adminDtTxt, CAMPUS_CD, SBJCT_CD, CATALOG_NBR, CLASS_SECTION_CD, CLASS_NUM, instrNm, SCHED_PRINT_INSTR, INSTRCTR_ROLE_CD, INSTRCTN_MODE_CD, SSR_COMP_CD, totEnrl_nowd, totEnrl_nowd_comb, combStat, SCTN_CMBND_LD, SESSION_CD, fcqStDt, fcqEnDt, CRSE_LD, assoc_class_secID, deptOrgID)

#########################################################################
# formatting audit files
#########################################################################

# update column names
colnames(auditprep) <- c('Admin_FCQs', 'Reason_for_NO', 'FCQ_admin_dates', 'Campus', 'Subject', 'Course', 'Section', 'Class_num', 'Instructor', 'Sched_print_instr', 'Instr_role', 'Instrctn_mode', 'Class_comp_code', 'Sect_enrlmnt', 'Comb_sect_enrlmnt', 'Comb_sect_type', 'Comb_spons_sect', 'Session', 'Class_meet_start_dt', 'Class_meet_end_dt', 'Course_title', 'Assoc_class', 'deptOrgID')

# adminInd: convert 1/0 to YES/NO
auditprep1 <- auditprep %>%
  mutate(Admin_FCQs = case_when(
    Admin_FCQs == 1 ~ 'YES',
    TRUE ~ 'NO')) %>%
  arrange(Subject, Course, Section)

#########################################################################
# printing audit files
#########################################################################

# setwd() to shared folder
setwd('C:\\Users\\darcange\\OneDrive - UCB-O365\\CourseAudit')

# all class daily + backup
appenddt <- Sys.Date()
sheetdate <- paste0('audit', format(appenddt, format = '%m%d%y'))

# create style for classes not getting FCQs
noadmin <- createStyle(fontColour = '#FF0000', fontSize = 10)

# create style for top row
Heading <- createStyle(textDecoration = 'bold', fgFill = '#FFFFCC', border = 'TopBottomLeftRight')

# workbook call begins here
audit_all <- createWorkbook()
addWorksheet(audit_all, sheetdate, gridLines = TRUE)
writeData(audit_all, sheetdate, auditprep1, withFilter = TRUE)

# identify which rows are not getting FCQs (e.g., need to be formatted)
noRows = data.frame(which(auditprep1 == 'NO', arr.ind=TRUE))

# freeze top row
freezePane(audit_all, sheetdate, firstActiveRow = 2, firstActiveCol = 1)

# add style to header
addStyle(audit_all, sheetdate, cols = 1:ncol(auditprep1), rows = 1, style = Heading)

# add style to 'NO' rows
addStyle(audit_all, sheetdate, cols = 1:ncol(auditprep1), rows = noRows[,1]+1, style = noadmin, gridExpand = TRUE)

# daily (no date, goes to shared drive)
saveWorkbook(audit_all, paste0('audit', term_cd, '_all', '.xlsx'), overwrite = TRUE)

# backup (has date, goes to L drive)
saveWorkbook(audit_all, paste0('L:\\mgt\\FCQ\\CourseAudit\\', term_cd, '\\audit', term_cd, '_', format(appenddt, format = '%m%d%y'), '.xlsx'), overwrite = TRUE)

#########################################################################
# generate audit files by campus
#########################################################################

###############
# BD AUDIT FILE
bdaud <- auditprep1 %>%
  filter(Campus %in% (c('BLD3', 'BLDR'))) %>%
  arrange(Subject, Course, Section)

# create workbook
audit_bd <- createWorkbook()
addWorksheet(audit_bd, sheetdate, gridLines = TRUE)
writeData(audit_bd, sheetdate, bdaud, withFilter = TRUE)

# identify which rows are not getting FCQs (e.g., need to be formatted)
noRows = data.frame(which(bdaud == 'NO', arr.ind=TRUE))

# freeze top row
freezePane(audit_bd, sheetdate, firstActiveRow = 2, firstActiveCol = 1)

# add style to header
addStyle(audit_bd, sheetdate, cols = 1:ncol(bdaud), rows = 1, style = Heading)

# add style to 'NO' rows
addStyle(audit_bd, sheetdate, cols = 1:ncol(bdaud), rows = noRows[,1]+1, style = noadmin, gridExpand = TRUE)

saveWorkbook(audit_bd, paste0('audit', term_cd, '_campusBD', '.xlsx'), overwrite = TRUE)

###############
# CE AUDIT FILE
ceaud <- auditprep1 %>%
  filter(Campus == 'CEPS') %>%
  arrange(Subject, Course, Section)

# create workbook
audit_ce <- createWorkbook()
addWorksheet(audit_ce, sheetdate, gridLines = TRUE)
writeData(audit_ce, sheetdate, ceaud, withFilter = TRUE)

# identify which rows are not getting FCQs (e.g., need to be formatted)
noRows = data.frame(which(ceaud == 'NO', arr.ind=TRUE))

# freeze top row
freezePane(audit_ce, sheetdate, firstActiveRow = 2, firstActiveCol = 1)

# add style to header
addStyle(audit_ce, sheetdate, cols = 1:ncol(ceaud), rows = 1, style = Heading)

# add style to 'NO' rows
addStyle(audit_ce, sheetdate, cols = 1:ncol(ceaud), rows = noRows[,1]+1, style = noadmin, gridExpand = TRUE)

saveWorkbook(audit_ce, paste0('audit', term_cd, '_campusCE', '.xlsx'), overwrite = TRUE)

###############
# DN AUDIT FILE
dnaud <- auditprep1 %>%
  filter(Campus %in% (c('DC', 'EXSTD'))) %>%
  arrange(Subject, Course, Section)

# create workbook
audit_dn <- createWorkbook()
addWorksheet(audit_dn, sheetdate, gridLines = TRUE)
writeData(audit_dn, sheetdate, dnaud, withFilter = TRUE)

# identify which rows are not getting FCQs (e.g., need to be formatted)
noRows = data.frame(which(dnaud == 'NO', arr.ind=TRUE))

# freeze top row
freezePane(audit_dn, sheetdate, firstActiveRow = 2, firstActiveCol = 1)

# add style to header
addStyle(audit_dn, sheetdate, cols = 1:ncol(dnaud), rows = 1, style = Heading)

# add style to 'NO' rows
addStyle(audit_dn, sheetdate, cols = 1:ncol(dnaud), rows = noRows[,1]+1, style = noadmin, gridExpand = TRUE)

saveWorkbook(audit_dn, paste0('audit', term_cd, '_campusDN', '.xlsx'), overwrite = TRUE)

###############
# MC AUDIT FILE
mcaud <- auditprep1 %>%
  filter(Campus == 'AMC') %>%
  arrange(Subject, Course, Section)

# create workbook
audit_mc <- createWorkbook()
addWorksheet(audit_mc, sheetdate, gridLines = TRUE)
writeData(audit_mc, sheetdate, mcaud, withFilter = TRUE)

# identify which rows are not getting FCQs (e.g., need to be formatted)
noRows = data.frame(which(mcaud == 'NO', arr.ind=TRUE))

# freeze top row
freezePane(audit_mc, sheetdate, firstActiveRow = 2, firstActiveCol = 1)

# add style to header
addStyle(audit_mc, sheetdate, cols = 1:ncol(mcaud), rows = 1, style = Heading)

# add style to 'NO' rows
addStyle(audit_mc, sheetdate, cols = 1:ncol(mcaud), rows = noRows[,1]+1, style = noadmin, gridExpand = TRUE)

saveWorkbook(audit_mc, paste0('audit', term_cd, '_campusMC', '.xlsx'), overwrite = TRUE)

#########################################################################
# generate audit files by dept
#########################################################################

auditprep1 <- auditprep1 %>%
  arrange(Subject, Course, Section)

deptlst <- split(auditprep1,auditprep1$deptOrgID)

# create style for classes not receiving FCQs
noadmin <- createStyle(fontColour = '#FF0000', fontSize = 10)

# create style for top row
Heading <- createStyle(textDecoration = 'bold', fgFill = '#FFFFCC', border = 'TopBottomLeftRight')

auditxl <- function(x, y) {
  bydept <- createWorkbook()
  addWorksheet(bydept, sheetdate, gridLines = TRUE)
  writeData(bydept, sheetdate, x, withFilter = TRUE)

    # identify which rows are NO admin (e.g., need to be formatted)
  noRows = data.frame(which(x$Admin_FCQs == 'NO', arr.ind=FALSE))

  # freeze top row
  freezePane(bydept, sheetdate, firstActiveRow = 2, firstActiveCol = 1)

  # add style to header
  addStyle(bydept, sheetdate, cols = 1:ncol(x), rows = 1, style = Heading)

  # add style to 'NO' rows
  addStyle(bydept, sheetdate, cols = 1:ncol(x), rows = noRows[,1]+1, style = noadmin, gridExpand = TRUE)

  saveWorkbook(bydept,paste0('audit',term_cd,'_',y,'.xlsx'), overwrite = TRUE)
}

mapply(auditxl, deptlst, names(deptlst))
