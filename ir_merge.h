///
///
/// FACILITY    : merge 3 sources of ir cdr(TAP, NRTRDE and SCP) of the same/duplicated cdr to get only maximum charge/duration out of 3 sources
///
/// FILE NAME   : ir_merge.h
///
/// AUTHOR      : Thanakorn Nitipiromchai
///
/// CREATE DATE : 31-May-2019
///
/// CURRENT VERSION NO : 1.0
///
/// LAST RELEASE DATE  : 31-May-2019
///
/// MODIFICATION HISTORY :
///     1.0         31-May-2019     First Version
///     1.1.0       17-Sep-2019     Add keep state, purge old data feature and flushes logState
///
///
#ifndef __IR_RATE_H__
#define __IR_RATE_H__

#ifdef  __cplusplus
    extern "C" {
#endif
#include <ftw.h>

#define _APP_NAME_              "ir_merge"
#define _APP_VERS_              "1.1.0"

#define     TYPE_TAP            "TAP"
#define     TYPE_NRT            "NRT"
#define     TYPE_SCP            "SCP"

#define     TYPE_GPRS           "18"
#define     TYPE_VOICE_MO       "20"
#define     TYPE_SMS_MO         "21"
#define     TYPE_VOICE_MT       "30"
#define     TYPE_SMS_MT         "31"

#define     STATE_SUFF          ".proclist"
#define     ALERT_SUFF          ".alrt"
// ----- INI Parameters -----
// All Section
typedef enum {
    E_INPUT = 0,
    E_OUTPUT,
    E_OUT_TAP,
    E_OUT_NRT,
    E_OUT_SCP,
    E_COMMON,
    E_DBCONN,
    E_NOF_SECTION
} E_INI_SECTION;

typedef enum {
    // INPUT Section
    E_IR_INP_DIR = 0,
    E_IR_FPREF,
    E_IR_FSUFF,
    E_NOF_PAR_INPUT
} E_INI_INPUT_SEC;

typedef enum {
    // OUTPUT Section
    E_OUT_DIR = 0,
    E_OUT_FPREF,
    E_OUT_FSUFF,
    E_SRC_TO_MERGE,
    E_CT_TO_MERGE,
    E_NOF_PAR_OUTPUT
} E_INI_OUTPUT_SEC;

typedef enum {
    E_GEN_OUT = 0,
    E_CALL_TYPE,
    E_CHARGE_TYPE,
    E_NOF_PAR_GEN
} E_INI_GEN_SEC;

typedef enum {
    // COMMON Section
    E_TMP_DIR = 0,
    E_STATE_DIR,
    E_KEEP_STATE_DAY,
    E_SKIP_OLD_FILE,
    E_LOG_DIR,
    E_LOG_LEVEL,
    E_ALRT_DB,
    E_ALRT_DB_DIR,
    E_SLEEP_SEC,
    E_SLACK_SEC,
    E_NOF_PAR_COMMON
} E_INI_COMMON_SEC;

typedef enum {
    // DB Section
    E_SUB_USER = 0,
    E_SUB_PASSWORD,
    E_SUB_DB_SID,
    E_RETRY_COUNT,
    E_RETRY_WAIT,
    E_NOF_PAR_DBCONN
} E_INI_DBCONN_SEC;


int     buildSnapFile(const char *snapfile);
int     chkSnapVsState(const char *snap);
int     _chkIrFile(const char *fpath, const struct stat *info, int typeflag, struct FTW *ftwbuf);
void    procSynFiles(const char *dir, const char *fname, int seq, long cont_pos);
int     olderThan(int day, const char *sdir, const char *fname);

void    cloneInput(char *pbuf[]);
int     mergeCdr(char *pbuf[], int bsize);
int     wrtOutIrCommon(FILE **ofp);
int     wrtAlrtDbConnFail(const char *odir, const char *fname, const char *dbsvr);

int     logState(const char *dir, const char *file_name);
void    clearOldState();
void    purgeOldData(const char *old_state);
int     readConfig(int argc, char *argv[]);
void    logHeader();
void    printUsage();
int     validateIni();
int     _ini_callback(const char *section, const char *key, const char *value, void *userdata);
void    makeIni();

void    printCommon();

#ifdef  __cplusplus
    }
#endif


#endif  // __IR_RATE_H__

