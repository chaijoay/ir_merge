///
///
/// FACILITY    : db utility for rating and mapping of ir cdr
///
/// FILE NAME   : ir_rate_dbu.h
///
/// AUTHOR      : Thanakorn Nitipiromchai
///
/// CREATE DATE : 15-May-2019
///
/// CURRENT VERSION NO : 1.1.2
///
/// LAST RELEASE DATE  : 21-Nov-2019
///
/// MODIFICATION HISTORY :
///     1.0         15-May-2019     First Version
///     1.1.2       21-Nov-2019     fix state file checking
///     1.1.3       18-Sep-2020     check further on call_number similarity before merging
///
///
#ifndef __IR_RATE_DBU_H__
#define __IR_RATE_DBU_H__

#ifdef  __cplusplus
    extern "C" {
#endif

#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include <stdlib.h>

#include "strlogutl.h"
#include "glb_str_def.h"
#include "ir_field_def.h"


#define NOT_FOUND               1403
#define KEY_DUP                 -1
#define DEADLOCK                -60
#define FETCH_OUTOFSEQ          -1002

#define SIZE_GEN_STR            100
#define SIZE_SQL_STR            1024

#define SIZE_PMN_CODE           10
#define SIZE_COUNTRY_CODE       10
#define SIZE_CHRG_TYPE          2
#define SIZE_IDD_ACC_CODE       10
#define SIZE_BNO                50
#define SIZE_ANO                20

#define     PREPAID             "0"
#define     POSTPAID            "1"

#define     BIT_MIS_IMSI        0x001
#define     BIT_MIS_PMN         0x002

#ifdef	__cplusplus
extern "C" {
#endif


int   connectDbSub(char *szDbUsr, char *szDbPwd, char *szDbSvr, int nRetryCnt, int nRetryWait);
void  disconnSub(char *szDbSvr);
void  doCommit();

int   checkAndMerge(ST_IR_COMMON *ircdr, int slack_time, const char *src2merge, int proc_id);
int   insertIrCdr(ST_IR_COMMON *ircdr, int proc_id);
int   purgeOldCdr(int days_to_keep, int proc_id);


#ifdef  __cplusplus
    }
#endif

#endif
