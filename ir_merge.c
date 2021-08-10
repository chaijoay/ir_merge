///
///
/// FACILITY    : merge 3 sources of ir cdr(TAP, NRTRDE and SCP) of the same/duplicated cdr to get only maximum charge/duration out of 3 sources
///
/// FILE NAME   : ir_merge.c
///
/// AUTHOR      : Thanakorn Nitipiromchai
///
/// CREATE DATE : 31-May-2019
///
/// CURRENT VERSION NO : 1.1.2
///
/// LAST RELEASE DATE  : 21-Nov-2019
///
/// MODIFICATION HISTORY :
///     1.0         31-May-2019     First Version
///     1.1.0       17-Sep-2019     Add keep state, purge old data feature and flushes logState
///     1.1.2       21-Nov-2019     fix state file checking
///
///
#define _XOPEN_SOURCE           700         // Required under GLIBC for nftw()
#define _POSIX_C_SOURCE         200809L
#define _XOPEN_SOURCE_EXTENDED  1

#include <time.h>
#include <errno.h>
#include <stdio.h>
#include <dirent.h>
#include <libgen.h>
#include <string.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <sys/types.h>

#include "minIni.h"
#include "procsig.h"
#include "ir_merge.h"
#include "strlogutl.h"
#include "ir_merge_dbu.h"
#include "ir_field_def.h"

#define  TMPSUF     "._tmp"

ST_IR_COMMON gIrCommon;
char gszOutFname[SIZE_ITEM_L+1];
char gszAppName[SIZE_ITEM_S+1];
char gszIniFile[SIZE_FULL_NAME+1];
char gszToday[SIZE_DATE_ONLY+1];
char *pbuf_rec[NOF_IR_FLD+1];
FILE *gfpSnap;
FILE *gfpState;
int gnSnapCnt;
int gnLenPreIr;
int gnLenSufIr;
int gnPrcId;
int gnDayToKeep;

const char gszIniStrSection[E_NOF_SECTION][SIZE_ITEM_T] = {
    "INPUT",
    "OUTPUT",
    "OUTPUT_TAP",
    "OUTPUT_NRT",
    "OUTPUT_SCP",
    "COMMON",
    "DB_CONNECTION"
};

const char gszIniStrInput[E_NOF_PAR_INPUT][SIZE_ITEM_T] = {
    "IR_INPUT_DIR",
    "IR_FILE_PREFIX",
    "IR_FILE_SUFFIX"
};

const char gszIniStrOutput[E_NOF_PAR_OUTPUT][SIZE_ITEM_T] = {
    "OUTPUT_DIR",
    "OUT_FILE_PREFIX",
    "OUT_FILE_SUFFIX",
    "SRC_TO_MERGE",
    "CALL_TYPE_TO_MERGE"
};

const char gszIniStrGenOut[E_NOF_PAR_GEN][SIZE_ITEM_T] = {
    "GEN_OUTPUT",
    "CALL_TYPE",
    "CHARGE_TYPE",
};

const char gszIniStrCommon[E_NOF_PAR_COMMON][SIZE_ITEM_T] = {
    "TMP_DIR",
    "STATE_DIR",
    "KEEP_STATE_DAY",
    "SKIP_OLD_FILE",
    "LOG_DIR",
    "LOG_LEVEL",
    "ALRT_DBCON_FAIL",
    "ALRT_DBCON_DIR",
    "SLEEP_SECOND",
    "SLACK_TIME_SEC"
};

const char gszIniStrDbConn[E_NOF_PAR_DBCONN][SIZE_ITEM_T] = {
    "SUB_USER_NAME",
    "SUB_PASSWORD",
    "SUB_DB_SID",
    "RETRY_COUNT",
    "RETRY_WAIT"
};

char gszIniParInput[E_NOF_PAR_INPUT][SIZE_ITEM_L];
char gszIniParOutput[E_NOF_PAR_OUTPUT][SIZE_ITEM_L];
char gszIniParGenTap[E_NOF_PAR_GEN][SIZE_ITEM_T];
char gszIniParGenNrt[E_NOF_PAR_GEN][SIZE_ITEM_T];
char gszIniParGenScp[E_NOF_PAR_GEN][SIZE_ITEM_T];
char gszIniParCommon[E_NOF_PAR_COMMON][SIZE_ITEM_L];
char gszIniParDbConn[E_NOF_PAR_DBCONN][SIZE_ITEM_L];

int main(int argc, char *argv[])
{
    FILE *ifp = NULL;
    gfpState = NULL;
    char szSnap[SIZE_ITEM_L], snp_line[SIZE_BUFF];
    int retryBldSnap = 3;
    int nInpFileCntDay = 0, nInpFileCntRnd = 0;
    time_t t_bat_start = 0, t_bat_stop = 0;
    int seq = 0;

    memset(gszAppName, 0x00, sizeof(gszAppName));
    memset(gszIniFile, 0x00, sizeof(gszIniFile));
    memset(gszToday, 0x00, sizeof(gszToday));

    // 1. read ini file
    if ( readConfig(argc, argv) != SUCCESS ) {
        return EXIT_FAILURE;
    }

    if ( procLock(gszAppName, E_CHK) != SUCCESS ) {
        fprintf(stderr, "another instance of %s is running\n", gszAppName);
        return EXIT_FAILURE;
    }

    if ( handleSignal() != SUCCESS ) {
        fprintf(stderr, "init handle signal failed: %s\n", getSigInfoStr());
        return EXIT_FAILURE;
    }

    if ( startLogging(gszIniParCommon[E_LOG_DIR], gszAppName, atoi(gszIniParCommon[E_LOG_LEVEL])) != SUCCESS ) {
       return EXIT_FAILURE;
    }

    if ( validateIni() == FAILED ) {
        return EXIT_FAILURE;
    }
    logHeader();

    char ir_file[SIZE_ITEM_L];  memset(ir_file, 0x00, sizeof(ir_file));
    char ir_type[10];           memset(ir_type, 0x00, sizeof(ir_type));
    long cont_pos = 0L;

    cont_pos = checkPoint(NULL, ir_file, ir_type, gszIniParCommon[E_TMP_DIR], gszAppName, E_CHK);

    strcpy(gszToday, getSysDTM(DTM_DATE_ONLY));
    // Main processing loop
    while ( TRUE ) {

        procLock(gszAppName, E_SET);

        if ( isTerminated() == TRUE ) {
            break;
        }

        // main process flow:
        // 1. build snapshot file -> list all files to be processed.
        // 2. connect to dbs (and also retry if any)

        // start over from step 1
        gnSnapCnt = 0;
        memset(szSnap, 0x00, sizeof(szSnap));
        sprintf(szSnap, "%s/%s.snap", gszIniParCommon[E_TMP_DIR], gszAppName);
        if ( cont_pos <= 0 ) {
            if ( buildSnapFile(szSnap) != SUCCESS ) {
                if ( --retryBldSnap <= 0 ) {
                    fprintf(stderr, "retry build snap exceeded\n");
                    break;
                }
                sleep(10);
                continue;
            }
            retryBldSnap = 3;
            // check snap against state file
            gnSnapCnt = chkSnapVsState(szSnap);
        }
        if ( gnSnapCnt < 0 ) {
            break;  // There are some problem in reading state file
        }

        if ( gnSnapCnt > 0 || cont_pos > 0 ) {

            if ( (ifp = fopen(szSnap, "r")) == NULL ) {
                writeLog(LOG_SYS, "unable open %s for reading (%s)", szSnap, strerror(errno));
                break;
            }
            else {
                if ( connectDbSub(gszIniParDbConn[E_SUB_USER], gszIniParDbConn[E_SUB_PASSWORD], gszIniParDbConn[E_SUB_DB_SID], atoi(gszIniParDbConn[E_RETRY_COUNT]), atoi(gszIniParDbConn[E_RETRY_WAIT])) != SUCCESS ) {
                    if ( *gszIniParCommon[E_ALRT_DB] == 'Y' ) {
                        wrtAlrtDbConnFail(gszIniParCommon[E_ALRT_DB_DIR], gszToday, gszIniParDbConn[E_SUB_DB_SID]);
                    }
                    break;
                }

                if ( cont_pos > 0 ) {   // continue from last time first
                    writeLog(LOG_INF, "continue process %s from last time", ir_file);
                    seq++;
                    procSynFiles(dirname(ir_file), basename(ir_file), seq, cont_pos);
                    cont_pos = 0;
                    continue;           // back to build snap to continue normal loop
                }

                nInpFileCntRnd = 0;
                t_bat_start = time(NULL);
                while ( fgets(snp_line, sizeof(snp_line), ifp) ) {

                    if ( isTerminated() == TRUE ) {
                        break;
                    }

                    trimStr(snp_line);  // snap record format => <path>|<filename>
                    char sdir[SIZE_ITEM_M], sfname[SIZE_ITEM_M];
                    memset(sdir, 0x00, sizeof(sdir));
                    memset(sfname, 0x00, sizeof(sfname));

                    getTokenItem(snp_line, 1, '|', sdir);
                    getTokenItem(snp_line, 2, '|', sfname);

                    if ( ! olderThan(atoi(gszIniParCommon[E_SKIP_OLD_FILE]), sdir, sfname) ) {
                        ( ++seq > 999 ? seq = 0 : seq );
                        procSynFiles(sdir, sfname, seq, 0L);
                    }

                    nInpFileCntDay++;
                    nInpFileCntRnd++;

                }
                t_bat_stop = time(NULL);
                writeLog(LOG_INF, "total processed files for this round=%d round_time_used=%d sec", nInpFileCntRnd, (t_bat_stop - t_bat_start));

                fclose(ifp);

                if ( strcmp(gszToday, getSysDTM(DTM_DATE_ONLY)) ) {
                    purgeOldCdr(gnDayToKeep, gnPrcId);  // purge only at end of day
                }
                disconnSub(gszIniParDbConn[E_SUB_DB_SID]);

            }
        }
        else {
            writeLog(LOG_INF, "no new input file");
        }

        if ( isTerminated() == TRUE ) {
            if ( gfpState != NULL ) {
                fclose(gfpState);
                gfpState = NULL;
            }
            break;
        }
        else {
            writeLog(LOG_INF, "sleep %s sec", gszIniParCommon[E_SLEEP_SEC]);
            sleep(atoi(gszIniParCommon[E_SLEEP_SEC]));
        }

        if ( strcmp(gszToday, getSysDTM(DTM_DATE_ONLY)) ) {
            if ( gfpState != NULL ) {
                fclose(gfpState);
                gfpState = NULL;
            }
            writeLog(LOG_INF, "total processed files for today=%d", nInpFileCntDay);
            strcpy(gszToday, getSysDTM(DTM_DATE_ONLY));
            manageLogFile();
            clearOldState();
            nInpFileCntDay = 0;
        }

    }
    procLock(gszAppName, E_CLR);
    writeLog(LOG_INF, "%s", getSigInfoStr());
    writeLog(LOG_INF, "------- %s %d process completely stop -------", _APP_NAME_, gnPrcId);
    stopLogging();

    return EXIT_SUCCESS;

}

int buildSnapFile(const char *snapfile)
{
    char cmd[SIZE_BUFF];
    gnSnapCnt = 0;

    gnLenPreIr = strlen(gszIniParInput[E_IR_FPREF]);
    gnLenSufIr = strlen(gszIniParInput[E_IR_FSUFF]);

    // open snap file for writing
    if ( (gfpSnap = fopen(snapfile, "w")) == NULL ) {
        writeLog(LOG_SYS, "unable to open %s for writing: %s\n", snapfile, strerror(errno));
        return FAILED;
    }

    // recursively walk through directories and file and check matching
    writeLog(LOG_INF, "scaning sync file in directory %s", gszIniParInput[E_IR_INP_DIR]);
    if ( nftw(gszIniParInput[E_IR_INP_DIR], _chkIrFile, 32, FTW_DEPTH) ) {
        writeLog(LOG_SYS, "unable to read path %s: %s\n", gszIniParInput[E_IR_INP_DIR], strerror(errno));
        fclose(gfpSnap);
        gfpSnap = NULL;
        return FAILED;
    }

    fclose(gfpSnap);
    gfpSnap = NULL;

    // if there are sync files then sort the snap file
    if ( gnSnapCnt > 0 ) {
        memset(cmd, 0x00, sizeof(cmd));
        sprintf(cmd, "sort -T %s %s > %s.tmp 2>/dev/null", gszIniParCommon[E_TMP_DIR], snapfile, snapfile);
writeLog(LOG_DB3, "buildSnapFile cmd '%s'", cmd);
        if ( system(cmd) != SUCCESS ) {
            writeLog(LOG_SYS, "cannot sort file %s (%s)", snapfile, strerror(errno));
            sprintf(cmd, "rm -f %s %s.tmp", snapfile, snapfile);
            system(cmd);
            return FAILED;
        }
        sprintf(cmd, "mv %s.tmp %s 2>/dev/null", snapfile, snapfile);
writeLog(LOG_DB3, "buildSnapFile cmd '%s'", cmd);
        system(cmd);
    }
    else {
        writeLog(LOG_INF, "no input file");
    }

    return SUCCESS;

}

int chkSnapVsState(const char *snap)
{
    char cmd[SIZE_BUFF];
    char tmp_stat[SIZE_ITEM_L], tmp_snap[SIZE_ITEM_L];
    FILE *fp = NULL;

    memset(tmp_stat, 0x00, sizeof(tmp_stat));
    memset(tmp_snap, 0x00, sizeof(tmp_snap));
    memset(cmd, 0x00, sizeof(cmd));

    sprintf(tmp_stat, "%s/tmp_%s_XXXXXX", gszIniParCommon[E_TMP_DIR], gszAppName);
    sprintf(tmp_snap, "%s/osnap_%s_XXXXXX", gszIniParCommon[E_TMP_DIR], gszAppName);
    mkstemp(tmp_stat);
    mkstemp(tmp_snap);

	// close and flush current state file, in case it's opening
	if ( gfpState != NULL ) {
		fclose(gfpState);
		gfpState = NULL;
	}

    // create state file of current day just in case there is currently no any state file.
    sprintf(cmd, "touch %s/%s_%s%s", gszIniParCommon[E_STATE_DIR], gszAppName, gszToday, STATE_SUFF);
writeLog(LOG_DB3, "chkSnapVsState cmd '%s'", cmd);
    system(cmd);

    if ( chkStateAndConcat(tmp_stat) == SUCCESS ) {
        // sort all state files (<APP_NAME>_<PROC_TYPE>_<YYYYMMDD>.proclist) to tmp_stat file
        // state files format is <DIR>|<FILE_NAME>
        //sprintf(cmd, "sort -T %s %s/%s_*%s > %s 2>/dev/null", gszIniParCommon[E_TMP_DIR], gszIniParCommon[E_STATE_DIR], gszAppName, STATE_SUFF, tmp_stat);
        sprintf(cmd, "sort -T %s %s > %s.tmp 2>/dev/null", gszIniParCommon[E_TMP_DIR], tmp_stat, tmp_stat);
writeLog(LOG_DB3, "chkSnapVsState cmd '%s'", cmd);
        system(cmd);
    }
    else {
        unlink(tmp_stat);
        return FAILED;
    }

    // compare tmp_stat file(sorted all state files) with sorted first_snap to get only unprocessed new files list
    sprintf(cmd, "comm -23 %s %s.tmp > %s 2>/dev/null", snap, tmp_stat, tmp_snap);
writeLog(LOG_DB3, "chkSnapVsState cmd '%s'", cmd);
    system(cmd);
    sprintf(cmd, "rm -f %s %s.tmp", tmp_stat, tmp_stat);
writeLog(LOG_DB3, "chkSnapVsState cmd '%s'", cmd);
    system(cmd);

    sprintf(cmd, "mv %s %s", tmp_snap, snap);
writeLog(LOG_DB3, "chkSnapVsState cmd '%s'", cmd);
    system(cmd);

    // get record count from output file (snap)
    sprintf(cmd, "cat %s | wc -l", snap);
writeLog(LOG_DB3, "chkSnapVsState cmd '%s'", cmd);
    fp = popen(cmd, "r");
    fgets(tmp_stat, sizeof(tmp_stat), fp);
    pclose(fp);

    return atoi(tmp_stat);

}

int _chkIrFile(const char *fpath, const struct stat *info, int typeflag, struct FTW *ftwbuf)
{

    const char *fname = fpath + ftwbuf->base;
    int fname_len = strlen(fname);
    char path_only[SIZE_ITEM_L];

    if ( typeflag != FTW_F && typeflag != FTW_SL && typeflag != FTW_SLN )
        return 0;

    // matching file name prefix
    if ( strncmp(fname, gszIniParInput[E_IR_FPREF], gnLenPreIr) != 0 ) {
        return 0;
    }

    // matching file name suffix
    if ( strcmp(fname + (fname_len - gnLenSufIr), gszIniParInput[E_IR_FSUFF]) != 0 ) {
        return 0;
    }

    if ( !(info->st_mode & (S_IRUSR|S_IRGRP|S_IROTH)) ) {
        writeLog(LOG_WRN, "no read permission for %s skipped", fname);
        return 0;
    }

    memset(path_only, 0x00, sizeof(path_only));
    strncpy(path_only, fpath, ftwbuf->base - 1);

    gnSnapCnt++;
    fprintf(gfpSnap, "%s|%s\n", path_only, fname);    // write snap output format -> <DIR>|<FILE>
    return 0;

}

int logState(const char *dir, const char *file_name)
{
    int result = 0;
    if ( gfpState == NULL ) {
        char fstate[SIZE_ITEM_L];
        memset(fstate, 0x00, sizeof(fstate));
        sprintf(fstate, "%s/%s_%s%s", gszIniParCommon[E_STATE_DIR], gszAppName, gszToday, STATE_SUFF);
        gfpState = fopen(fstate, "a");
    }
    result = fprintf(gfpState, "%s|%s\n", dir, file_name);
    fflush(gfpState);
    return result;
}

void clearOldState()
{
    struct tm *ptm;
    time_t lTime;
    char tmp[SIZE_ITEM_L];
    char szOldestFile[SIZE_ITEM_S];
    char szOldestDate[SIZE_DATE_TIME_FULL+1];
    DIR *p_dir;
    struct dirent *p_dirent;
    int len1 = 0, len2 = 0;

    /* get oldest date to keep */
    time(&lTime);
    ptm = localtime( &lTime);
//printf("ptm->tm_mday = %d\n", ptm->tm_mday);
    ptm->tm_mday = ptm->tm_mday - atoi(gszIniParCommon[E_KEEP_STATE_DAY]);
//printf("ptm->tm_mday(after) = %d, keepState = %d\n", ptm->tm_mday, atoi(gszIniParCommon[E_KEEP_STATE_DAY]));
    lTime = mktime(ptm);
    ptm = localtime(&lTime);
    strftime(szOldestDate, sizeof(szOldestDate)-1, "%Y%m%d", ptm);
//printf("szOldestDate = %s\n", szOldestDate);

	writeLog(LOG_INF, "purge state file up to %s (keep %s days)", szOldestDate, gszIniParCommon[E_KEEP_STATE_DAY]);
    sprintf(szOldestFile, "%s%s", szOldestDate, STATE_SUFF);     // YYYYMMDD.proclist
    len1 = strlen(szOldestFile);
    if ( (p_dir = opendir(gszIniParCommon[E_STATE_DIR])) != NULL ) {
        while ( (p_dirent = readdir(p_dir)) != NULL ) {
            // state file name: <APP_NAME>_<PROC_TYPE>_YYYYMMDD.proclist
            if ( strcmp(p_dirent->d_name, ".") == 0 || strcmp(p_dirent->d_name, "..") == 0 )
                continue;
            if ( strstr(p_dirent->d_name, STATE_SUFF) != NULL &&
                 strstr(p_dirent->d_name, gszAppName) != NULL ) {

                len2 = strlen(p_dirent->d_name);
                // compare only last term of YYYYMMDD.proclist
                if ( strcmp(szOldestFile, (p_dirent->d_name + (len2-len1))) > 0 ) {
                    char old_state[SIZE_ITEM_L];
                    memset(old_state, 0x00, sizeof(old_state));
                    sprintf(old_state, "%s/%s", gszIniParCommon[E_STATE_DIR], p_dirent->d_name);

                    purgeOldData(old_state);

                    sprintf(tmp, "rm -f %s/%s 2>/dev/null", gszIniParCommon[E_STATE_DIR], p_dirent->d_name);
                    writeLog(LOG_INF, "remove state file: %s", p_dirent->d_name);
                    system(tmp);
                }
            }
        }
        closedir(p_dir);
    }
}

void purgeOldData(const char *old_state)
{
    FILE *ofp = NULL;
    char line[SIZE_ITEM_L], sdir[SIZE_ITEM_L], sfname[SIZE_ITEM_L], cmd[SIZE_ITEM_L];

    if ( (ofp = fopen(old_state, "r")) != NULL ) {
        memset(line, 0x00, sizeof(line));
        while ( fgets(line, sizeof(line),ofp) ) {
            memset(sdir,   0x00, sizeof(sdir));
            memset(sfname, 0x00, sizeof(sfname));
            memset(cmd,    0x00, sizeof(cmd));

            getTokenItem(line, 1, '|', sdir);
            getTokenItem(line, 2, '|', sfname);

            sprintf(cmd, "rm -f %s/%s", sdir, sfname);
            writeLog(LOG_DB3, "\told file %s/%s purged", sdir, sfname);
            system(cmd);
        }
        fclose(ofp);
        ofp = NULL;
    }
}

int readConfig(int argc, char *argv[])
{

    char appPath[SIZE_ITEM_L];
    char tmp[SIZE_ITEM_T];
    int key, i;

    memset(gszIniFile, 0x00, sizeof(gszIniFile));
    memset(gszAppName, 0x00, sizeof(gszAppName));
    memset(tmp, 0x00, sizeof(tmp));

    memset(gszIniParInput,  0x00, sizeof(gszIniParInput));
    memset(gszIniParOutput, 0x00, sizeof(gszIniParOutput));
    memset(gszIniParCommon, 0x00, sizeof(gszIniParCommon));
    memset(gszIniParDbConn, 0x00, sizeof(gszIniParDbConn));
    memset(gszIniParGenTap, 0x00, sizeof(gszIniParGenTap));
    memset(gszIniParGenNrt, 0x00, sizeof(gszIniParGenNrt));
    memset(gszIniParGenScp, 0x00, sizeof(gszIniParGenScp));

    strcpy(appPath, argv[0]);
    char *p = strrchr(appPath, '/');
    *p = '\0';

    for ( i = 1; i < argc; i++ ) {
        if ( strcmp(argv[i], "-n") == 0 ) {     // specified ini file
            strcpy(gszIniFile, argv[++i]);
        }
        else if ( strcmp(argv[i], "-i") == 0 ) {     // specified ini file
            strcpy(tmp, argv[++i]);
        }
        else if ( strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0 ) {
            printUsage();
            return FAILED;
        }
        else if ( strcmp(argv[i], "-mkini") == 0 ) {
            makeIni();
            return FAILED;
        }
        else {
            printUsage();
            return FAILED;
        }
    }

    if ( strlen(tmp) > 1 || *tmp - '0' < 0 || *tmp - '0' > 9 ) {
        printUsage();
        return FAILED;
    }

    gnPrcId = atoi(tmp);
    sprintf(gszAppName, "%s_%d", _APP_NAME_, gnPrcId);
    if ( gszIniFile[0] == '\0' ) {
        sprintf(gszIniFile, "%s/%s_%d.ini", appPath, _APP_NAME_, gnPrcId);
    }

    if ( access(gszIniFile, F_OK|R_OK) != SUCCESS ) {
        sprintf(gszIniFile, "%s/%s.ini", appPath, _APP_NAME_);
        if ( access(gszIniFile, F_OK|R_OK) != SUCCESS ) {
            fprintf(stderr, "unable to access ini file %s (%s)\n", gszIniFile, strerror(errno));
            return FAILED;
        }
    }

    // Read config of INPUT Section
    for ( key = 0; key < E_NOF_PAR_INPUT; key++ ) {
        ini_gets(gszIniStrSection[E_INPUT], gszIniStrInput[key], "NA", gszIniParInput[key], sizeof(gszIniParInput[key]), gszIniFile);
    }

    // Read config of OUTPUT Section
    for ( key = 0; key < E_NOF_PAR_OUTPUT; key++ ) {
        ini_gets(gszIniStrSection[E_OUTPUT], gszIniStrOutput[key], "NA", gszIniParOutput[key], sizeof(gszIniParOutput[key]), gszIniFile);
    }
    for ( key = 0; key < E_NOF_PAR_GEN; key++ ) {
        ini_gets(gszIniStrSection[E_OUT_TAP], gszIniStrGenOut[key], "NA", gszIniParGenTap[key], sizeof(gszIniParGenTap[key]), gszIniFile);
    }
    for ( key = 0; key < E_NOF_PAR_GEN; key++ ) {
        ini_gets(gszIniStrSection[E_OUT_NRT], gszIniStrGenOut[key], "NA", gszIniParGenNrt[key], sizeof(gszIniParGenNrt[key]), gszIniFile);
    }
    for ( key = 0; key < E_NOF_PAR_GEN; key++ ) {
        ini_gets(gszIniStrSection[E_OUT_SCP], gszIniStrGenOut[key], "NA", gszIniParGenScp[key], sizeof(gszIniParGenScp[key]), gszIniFile);
    }

    // Read config of COMMON Section
    for ( key = 0; key < E_NOF_PAR_COMMON; key++ ) {
        ini_gets(gszIniStrSection[E_COMMON], gszIniStrCommon[key], "NA", gszIniParCommon[key], sizeof(gszIniParCommon[key]), gszIniFile);
    }

    // Read config of DB Connection Section
    for ( key = 0; key < E_NOF_PAR_DBCONN; key++ ) {
        ini_gets(gszIniStrSection[E_DBCONN], gszIniStrDbConn[key], "NA", gszIniParDbConn[key], sizeof(gszIniParDbConn[key]), gszIniFile);
    }

    return SUCCESS;

}

void logHeader()
{
    writeLog(LOG_INF, "---- Start %s (v%s) with following parameters ----", _APP_NAME_, _APP_VERS_);
    // print out all ini file
    ini_browse(_ini_callback, NULL, gszIniFile);
}

void printUsage()
{
    fprintf(stderr, "\nusage: %s version %s\n", _APP_NAME_, _APP_VERS_);
    fprintf(stderr, "\tmerge ir cdrs to get max usage out of those sources\n\n");
    fprintf(stderr, "%s.exe <-i <id>> [-n <ini_file>] [-mkini]\n", _APP_NAME_);
    fprintf(stderr, "\tid\tto specify process id (0-9) the id is also used to process ending no of imsi\n");
    fprintf(stderr, "\tini_file\tto specify ini file other than default ini\n");
    fprintf(stderr, "\t-mkini\t\tto create ini template\n");
    fprintf(stderr, "\n");

}

int validateIni()
{
    int result = SUCCESS;

    // ----- Input Section -----
    if ( access(gszIniParInput[E_IR_INP_DIR], F_OK|R_OK) != SUCCESS ) {
        result = FAILED;
        fprintf(stderr, "unable to access %s %s (%s)\n", gszIniStrInput[E_IR_INP_DIR], gszIniParInput[E_IR_INP_DIR], strerror(errno));
    }

    // ----- Output Section -----
    if ( access(gszIniParOutput[E_OUT_DIR], F_OK|R_OK) != SUCCESS ) {
        result = FAILED;
        fprintf(stderr, "unable to access %s %s (%s)\n", gszIniStrOutput[E_OUT_DIR], gszIniParOutput[E_OUT_DIR], strerror(errno));
    }
    if ( *gszIniParOutput[E_SRC_TO_MERGE] == '\0' || strcmp(gszIniParOutput[E_SRC_TO_MERGE], "NA") == 0 ) {
        result = FAILED;
        fprintf(stderr, "invalid %s %s (%s)\n", gszIniStrOutput[E_SRC_TO_MERGE], gszIniParOutput[E_SRC_TO_MERGE], strerror(errno));
    }
    if ( *gszIniParOutput[E_CT_TO_MERGE] == '\0' || strcmp(gszIniParOutput[E_CT_TO_MERGE], "NA") == 0 ) {
        result = FAILED;
        fprintf(stderr, "invalid %s %s (%s)\n", gszIniStrOutput[E_CT_TO_MERGE], gszIniParOutput[E_CT_TO_MERGE], strerror(errno));
    }

    // ----- Output TAP Section -----
    if ( *gszIniParGenTap[E_GEN_OUT] == 'Y' || *gszIniParGenTap[E_GEN_OUT] == 'y' ) {
        strcpy(gszIniParGenTap[E_GEN_OUT], "Y");
        if ( *gszIniParGenTap[E_CALL_TYPE] == '\0' || strcmp(gszIniParGenTap[E_CALL_TYPE], "NA") == 0 ) {
            result = FAILED;
            fprintf(stderr, "invalid %s %s (%s)\n", gszIniStrGenOut[E_CALL_TYPE], gszIniParGenTap[E_CALL_TYPE], strerror(errno));
        }
        if ( *gszIniParGenTap[E_CHARGE_TYPE] == '\0' || strcmp(gszIniParGenTap[E_CHARGE_TYPE], "NA") == 0 ) {
            result = FAILED;
            fprintf(stderr, "invalid %s %s (%s)\n", gszIniStrGenOut[E_CHARGE_TYPE], gszIniParGenTap[E_CHARGE_TYPE], strerror(errno));
        }

    }
    // ----- Output NRT Section -----
    if ( *gszIniParGenNrt[E_GEN_OUT] == 'Y' || *gszIniParGenNrt[E_GEN_OUT] == 'y' ) {
        strcpy(gszIniParGenNrt[E_GEN_OUT], "Y");
        if ( *gszIniParGenNrt[E_CALL_TYPE] == '\0' || strcmp(gszIniParGenNrt[E_CALL_TYPE], "NA") == 0 ) {
            result = FAILED;
            fprintf(stderr, "invalid %s %s (%s)\n", gszIniStrGenOut[E_CALL_TYPE], gszIniParGenNrt[E_CALL_TYPE], strerror(errno));
        }
        if ( *gszIniParGenNrt[E_CHARGE_TYPE] == '\0' || strcmp(gszIniParGenNrt[E_CHARGE_TYPE], "NA") == 0 ) {
            result = FAILED;
            fprintf(stderr, "invalid %s %s (%s)\n", gszIniStrGenOut[E_CHARGE_TYPE], gszIniParGenNrt[E_CHARGE_TYPE], strerror(errno));
        }

    }
    // ----- Output SCP Section -----
    if ( *gszIniParGenScp[E_GEN_OUT] == 'Y' || *gszIniParGenScp[E_GEN_OUT] == 'y' ) {
        strcpy(gszIniParGenScp[E_GEN_OUT], "Y");
        if ( *gszIniParGenScp[E_CALL_TYPE] == '\0' || strcmp(gszIniParGenScp[E_CALL_TYPE], "NA") == 0 ) {
            result = FAILED;
            fprintf(stderr, "invalid %s %s (%s)\n", gszIniStrGenOut[E_CALL_TYPE], gszIniParGenScp[E_CALL_TYPE], strerror(errno));
        }
        if ( *gszIniParGenScp[E_CHARGE_TYPE] == '\0' || strcmp(gszIniParGenScp[E_CHARGE_TYPE], "NA") == 0 ) {
            result = FAILED;
            fprintf(stderr, "invalid %s %s (%s)\n", gszIniStrGenOut[E_CHARGE_TYPE], gszIniParGenScp[E_CHARGE_TYPE], strerror(errno));
        }

    }

    // ----- Common Section -----
    if ( access(gszIniParCommon[E_TMP_DIR], F_OK|R_OK) != SUCCESS ) {
        result = FAILED;
        fprintf(stderr, "unable to access %s %s (%s)\n", gszIniStrCommon[E_TMP_DIR], gszIniParCommon[E_TMP_DIR], strerror(errno));
    }
    if ( access(gszIniParCommon[E_STATE_DIR], F_OK|R_OK) != SUCCESS ) {
        result = FAILED;
        fprintf(stderr, "unable to access %s %s (%s)\n", gszIniStrCommon[E_STATE_DIR], gszIniParCommon[E_STATE_DIR], strerror(errno));
    }
    if ( atoi(gszIniParCommon[E_KEEP_STATE_DAY]) <= 0 ) {
        result = FAILED;
        fprintf(stderr, "%s must be > 0 (%s)\n", gszIniStrCommon[E_KEEP_STATE_DAY], gszIniParCommon[E_KEEP_STATE_DAY]);
    }
    if ( atoi(gszIniParCommon[E_SKIP_OLD_FILE]) <= 0 ) {
        result = FAILED;
        fprintf(stderr, "%s must be > 0 (%s)\n", gszIniStrCommon[E_SKIP_OLD_FILE], gszIniParCommon[E_SKIP_OLD_FILE]);
    }
    if ( access(gszIniParCommon[E_LOG_DIR], F_OK|R_OK) != SUCCESS ) {
        result = FAILED;
        fprintf(stderr, "unable to access %s %s (%s)\n", gszIniStrCommon[E_LOG_DIR], gszIniParCommon[E_LOG_DIR], strerror(errno));
    }
    if ( *gszIniParCommon[E_ALRT_DB] == 'Y' || *gszIniParCommon[E_ALRT_DB] == 'y' ) {
        strcpy(gszIniParCommon[E_ALRT_DB], "Y");
        if ( access(gszIniParCommon[E_ALRT_DB_DIR], F_OK|R_OK) != SUCCESS ) {
            result = FAILED;
            fprintf(stderr, "unable to access %s %s (%s)\n", gszIniStrCommon[E_ALRT_DB_DIR], gszIniParCommon[E_ALRT_DB_DIR], strerror(errno));
        }
    }
    if ( atoi(gszIniParCommon[E_SLEEP_SEC]) <= 0 ) {
        result = FAILED;
        fprintf(stderr, "%s must be > 0 (%s)\n", gszIniStrCommon[E_SLEEP_SEC], gszIniParCommon[E_SLEEP_SEC]);
    }
    if ( atoi(gszIniParCommon[E_SLACK_SEC]) <= 0 ) {
        result = FAILED;
        fprintf(stderr, "%s must be > 0 (%s)\n", gszIniStrCommon[E_SLACK_SEC], gszIniParCommon[E_SLACK_SEC]);
    }

    // ----- Db Connection Section -----
    if ( *gszIniParDbConn[E_SUB_USER] == '\0' || strcmp(gszIniParDbConn[E_SUB_USER], "NA") == 0 ) {
        result = FAILED;
        fprintf(stderr, "invalid %s '%s'\n", gszIniStrDbConn[E_SUB_USER], gszIniParDbConn[E_SUB_USER]);
    }
    if ( *gszIniParDbConn[E_SUB_PASSWORD] == '\0' || strcmp(gszIniParDbConn[E_SUB_PASSWORD], "NA") == 0 ) {
        result = FAILED;
        fprintf(stderr, "invalid %s '%s'\n", gszIniStrDbConn[E_SUB_PASSWORD], gszIniParDbConn[E_SUB_PASSWORD]);
    }
    if ( *gszIniParDbConn[E_SUB_DB_SID] == '\0' || strcmp(gszIniParDbConn[E_SUB_DB_SID], "NA") == 0 ) {
        result = FAILED;
        fprintf(stderr, "invalid %s '%s'\n", gszIniStrDbConn[E_SUB_DB_SID], gszIniParDbConn[E_SUB_DB_SID]);
    }
    if ( atoi(gszIniParDbConn[E_RETRY_COUNT]) <= 0 ) {
        result = FAILED;
        fprintf(stderr, "%s must be > 0 (%s)\n", gszIniStrDbConn[E_RETRY_COUNT], gszIniParDbConn[E_RETRY_COUNT]);
    }
    if ( atoi(gszIniParDbConn[E_RETRY_WAIT]) <= 0 ) {
        result = FAILED;
        fprintf(stderr, "%s must be > 0 (%s)\n", gszIniStrDbConn[E_RETRY_WAIT], gszIniParDbConn[E_RETRY_WAIT]);
    }

    // number of days to purge cdr in db uses max value of wether E_SKIP_OLD_FILE or E_KEEP_STATE_DAY
    if ( atoi(gszIniParCommon[E_SKIP_OLD_FILE]) > atoi(gszIniParCommon[E_KEEP_STATE_DAY]) ) {
        gnDayToKeep = atoi(gszIniParCommon[E_SKIP_OLD_FILE]);
    }
    else {
        gnDayToKeep = atoi(gszIniParCommon[E_KEEP_STATE_DAY]);
    }

    return result;

}

int _ini_callback(const char *section, const char *key, const char *value, void *userdata)
{
    if ( strstr(key, "PASSWORD") ) {
        writeLog(LOG_INF, "[%s]\t%s = ********", section, key);
    }
    else {
        writeLog(LOG_INF, "[%s]\t%s = %s", section, key, value);
    }
    return 1;
}

void procSynFiles(const char *dir, const char *fname, int seq, long cont_pos)
{

    FILE *ifp_ir = NULL, *ofp_ir = NULL;
    char full_ir_name[SIZE_ITEM_L], ofile_id_str[SIZE_DATE_TIME_FULL+1];
    char read_rec[SIZE_BUFF], read_rec_ori[SIZE_BUFF];
    int parse_field_cnt = 0;
    int idx, mod_id, line_cnt = 0, proc_cnt = 0, skip_cnt = 0;
    time_t t_start = 0, t_stop = 0;

    memset(full_ir_name, 0x00, sizeof(full_ir_name));
    memset(read_rec, 0x00, sizeof(read_rec));
    memset(ofile_id_str, 0x00, sizeof(ofile_id_str));
    memset(gszOutFname, 0x00, sizeof(gszOutFname));
    sprintf(ofile_id_str, "%s_%03d_%d", getSysDTM(DTM_DATE_TIME), seq, gnPrcId);

    sprintf(full_ir_name, "%s/%s", dir, fname);
    if ( (ifp_ir = fopen(full_ir_name, "r")) == NULL ) {
        writeLog(LOG_SYS, "unable open read %s (%s)", full_ir_name, strerror(errno));
        return;
    }
    else {
        writeLog(LOG_INF, "processing file %s", fname);

        t_start = time(NULL);
        if ( cont_pos > 0 ) {
            fseek(ifp_ir, cont_pos, SEEK_SET);
        }

        sprintf(gszOutFname, "%s/%s_%s%s", gszIniParOutput[E_OUT_DIR], gszIniParOutput[E_OUT_FPREF], ofile_id_str, gszIniParOutput[E_OUT_FSUFF]);
        while ( fgets(read_rec, sizeof(read_rec), ifp_ir) ) {

            memset(pbuf_rec, 0x00, sizeof(pbuf_rec));
            memset(&gIrCommon, 0x00, sizeof(gIrCommon));
            memset(read_rec_ori, 0x00, sizeof(read_rec_ori));

            trimStr(read_rec);

            // safe original read record for later use, since getTokenAll modifies input string.
            strcpy(read_rec_ori, read_rec);
            line_cnt++;

            // parse field
            if ( (parse_field_cnt = getTokenAll(pbuf_rec, NOF_IR_FLD, read_rec, '|')) < NOF_IR_FLD ) {
                writeLog(LOG_ERR, "invalid field count %d expected %d", parse_field_cnt, NOF_IR_FLD);
writeLog(LOG_DB3, "%s", read_rec_ori);
                skip_cnt++;
                continue;
            }

            // check if the ending number of imsi is to be handled by this process or not
            idx = strlen(pbuf_rec[E_IMSI])-1;
            mod_id = pbuf_rec[E_IMSI][idx] - '0';

            if ( gnPrcId != mod_id ) {
writeLog(LOG_DB3, "skip unhandled imsi '%s'", pbuf_rec[E_IMSI]);
                skip_cnt++;
                continue;
            }
writeLog(LOG_DB3, "first mrg rec> stime(%s) calltype(%s) mobno(%s) imsi(%s) dur(%s) chg(%s[satang]) src(%s)"
        , pbuf_rec[E_ST_CALL_DATE], pbuf_rec[E_CALLTYPE], pbuf_rec[E_MOBILE_NO], pbuf_rec[E_IMSI]
        , pbuf_rec[E_DURATION], pbuf_rec[E_CHRG_ONE_TARIFF], pbuf_rec[E_ORI_SOURCE]);
// int i=0;
// for (i=0; i<NOF_IR_FLD; i++) {
    // printf("'%s' | ", pbuf_rec[i]);
// }
// printf("\n");

            // do merging cdr
            mergeCdr(pbuf_rec, NOF_IR_FLD);

            if ( wrtOutIrCommon(&ofp_ir) == SUCCESS ) {
                proc_cnt++;
            }
            else {
                skip_cnt++;
            }

            if ( (proc_cnt % 2000) == 0 && proc_cnt > 0 ) {
                writeLog(LOG_INF, "%10d records have been processed", proc_cnt);
                checkPoint(&ifp_ir, full_ir_name, "", gszIniParCommon[E_TMP_DIR], gszAppName, E_SET);
                doCommit();
            }

            if ( isTerminated() == TRUE ) {
                checkPoint(&ifp_ir, full_ir_name, "", gszIniParCommon[E_TMP_DIR], gszAppName, E_SET);
                break;
            }

        }
        if ( isTerminated() != TRUE ) {
            // clear check point in case whole file has been processed
            checkPoint(NULL, "", "", gszIniParCommon[E_TMP_DIR], gszAppName, E_CLR);
        }
        t_stop = time(NULL);

        if ( ifp_ir != NULL ) fclose(ifp_ir);
        if ( ofp_ir != NULL ) {
            char cmd[SIZE_FULL_NAME];   memset(cmd, 0x00, sizeof(cmd));
            fclose(ofp_ir);
            writeLog(LOG_INF, "processed %s -> %s", fname, basename(gszOutFname));
            sprintf(cmd, "mv %s%s %s", gszOutFname, TMPSUF, gszOutFname);
            system(cmd);

            chmod(gszOutFname, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);

            memset(cmd, 0x00, sizeof(cmd));
            char *f = strrchr(gszOutFname, '.');
            char s[SIZE_ITEM_L]; memset(s, 0x00, sizeof(s));
            strncpy(s, gszOutFname, (f - gszOutFname));
            sprintf(cmd, "touch %s.syn", s);
            system(cmd);

            sprintf(cmd, "%s.syn", s);
            chmod(cmd, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH|S_IWOTH);
        }

        logState(dir, fname);
        writeLog(LOG_INF, "%s done, process(id%d)=%d skip=%d total=%d file_time_used=%d sec", fname, gnPrcId, proc_cnt, skip_cnt, line_cnt, (t_stop - t_start));

        // if ( *gszIniParCommon[E_BCKUP] == 'Y' ) {
            // char cmd[SIZE_ITEM_L];
            // memset(cmd, 0x00, sizeof(cmd));
            // sprintf(cmd, "cp -p %s %s", full_ir_name, gszIniParCommon[E_BCKUP_DIR]);
            // syscmd(cmd);
        // }
        // unlink(full_ir_name);
    }

}

int olderThan(int day, const char *sdir, const char *fname)
{
    struct stat stat_buf;
    time_t systime = 0;
    int    result = FALSE;
    char   full_name[SIZE_ITEM_L];
    long   file_age = 0;
    long   bound = (long)(day * SEC_IN_DAY);

    memset(full_name, 0x00, sizeof(full_name));

    memset(&stat_buf, 0x00, sizeof(stat_buf));
    if ( !lstat(full_name, &stat_buf) ) {
        systime = time(NULL);
        file_age = (long)(systime - stat_buf.st_mtime);
        if ( file_age > bound ) {
            result = TRUE;
        }
    }
writeLog(LOG_DB2, "%s olderThan %d days (%ld sec) ", fname, day, file_age);
    return result;

}

void cloneInput(char *pbuf[])
{
    strcpy(gIrCommon.call_type,          pbuf[E_CALLTYPE]);
    strcpy(gIrCommon.imsi,               pbuf[E_IMSI]);
    strcpy(gIrCommon.st_call_date,       pbuf[E_ST_CALL_DATE]);
    strcpy(gIrCommon.st_call_time,       pbuf[E_ST_CALL_TIME]);
    strcpy(gIrCommon.duration,           pbuf[E_DURATION]);
    strcpy(gIrCommon.called_no,          pbuf[E_CALLED_NO]);
    strcpy(gIrCommon.charge,             pbuf[E_CHARGE]);
    strcpy(gIrCommon.pmn,                pbuf[E_PMN]);
    strcpy(gIrCommon.proc_dtm,           pbuf[E_PROC_DTM]);
    strcpy(gIrCommon.volume,             pbuf[E_VOLUME]);
    strcpy(gIrCommon.chrg_type,          pbuf[E_CHRG_TYPE]);
    strcpy(gIrCommon.company_name,       pbuf[E_COMPANY_NAME]);
    strcpy(gIrCommon.chrg_one_tariff,    pbuf[E_CHRG_ONE_TARIFF]);
    strcpy(gIrCommon.th_st_call_dtm,     pbuf[E_TH_ST_CALL_DTM]);
    strcpy(gIrCommon.called_no_type,     pbuf[E_CALLED_NO_TYPE]);
    strcpy(gIrCommon.risk_no_flg,        pbuf[E_RISK_NO_FLG]);
    strcpy(gIrCommon.risk_no_id,         pbuf[E_RISK_NO_ID]);
    strcpy(gIrCommon.billing_sys,        pbuf[E_BILLING_SYS]);
    strcpy(gIrCommon.start_dtm,          pbuf[E_START_DTM]);
    strcpy(gIrCommon.stop_dtm,           pbuf[E_STOP_DTM]);
    strcpy(gIrCommon.chrg_id,            pbuf[E_CHRG_ID]);
    strcpy(gIrCommon.utc_time,           pbuf[E_UTC_TIME]);
    strcpy(gIrCommon.total_call_evt_dur, pbuf[E_TOTAL_EVT_DUR]);
    strcpy(gIrCommon.ori_rec_type,       pbuf[E_ORI_REC_TYPE]);
    strcpy(gIrCommon.mobile_no,          pbuf[E_MOBILE_NO]);
    strcpy(gIrCommon.imei,               pbuf[E_IMEI]);
    strcpy(gIrCommon.ori_source,         pbuf[E_ORI_SOURCE]);
    strcpy(gIrCommon.ori_filename,       pbuf[E_ORI_FILENAME]);
    strcpy(gIrCommon.country_code,       pbuf[E_COUNTRY_CODE]);
    strcpy(gIrCommon.ori_duration,       pbuf[E_DURATION]);
    strcpy(gIrCommon.ori_one_charge,     pbuf[E_CHRG_ONE_TARIFF]);
    strcpy(gIrCommon.ori_volume,         pbuf[E_VOLUME]);
    strcpy(gIrCommon.pmn_name,           pbuf[E_PMN_NAME]);
    strcpy(gIrCommon.roam_country,       pbuf[E_ROAM_COUNTRY]);
    strcpy(gIrCommon.roam_region,        pbuf[E_ROAM_REGION]);
}

int mergeCdr(char *pbuf[], int bsize)
{
    int result = SUCCESS;
    char dtm[SIZE_DATE_TIME];

// int i=0;
// printf("in mergeCdr\n\t");
// for (i=0; i<NOF_IR_FLD; i++) {
    // printf("'%s' | ", pbuf[i]);
// }
// printf("\n");

    cloneInput(pbuf);   // copy pbuf to gIrCommon

    if ( strstr(gszIniParOutput[E_SRC_TO_MERGE], gIrCommon.ori_source) != NULL
        && strstr(gszIniParOutput[E_CT_TO_MERGE], gIrCommon.call_type) != NULL ) {
        memset(dtm, 0x00, sizeof(dtm));
        strncpy(dtm, gIrCommon.start_dtm, 8);           // copy YYYYMMDD, ircdr->start_dtm is in format of "YYYYMMDD HH:MI:SS"
        strncat(dtm, gIrCommon.start_dtm+9, 2);         // cat HH
        strncat(dtm, gIrCommon.start_dtm+12, 2);        // cat MI
        strncat(dtm, gIrCommon.start_dtm+15, 2);        // cat SS
        gIrCommon.start_dtm_time = dateStr2TimeT(dtm);  // convert start dtm string to time_t for further db insertion

        checkAndMerge(&gIrCommon, atoi(gszIniParCommon[E_SLACK_SEC]), gszIniParOutput[E_SRC_TO_MERGE], gnPrcId);

        insertIrCdr(&gIrCommon, gnPrcId);
    }
    else {
writeLog(LOG_DB3, "skip merge src(%s) calltype(%s)", gIrCommon.ori_source, gIrCommon.call_type);
    }

    return result;

}

int wrtOutIrCommon(FILE **ofp)
{
    char full_irfile[SIZE_ITEM_L];
    int gen_output = FALSE;
    int result = FAILED;

    if ( *ofp == NULL ) {
        memset(full_irfile, 0x00, sizeof(full_irfile));
        sprintf(full_irfile, "%s%s", gszOutFname, TMPSUF);
        if ( (*ofp = fopen(full_irfile, "a")) == NULL ) {
            writeLog(LOG_SYS, "unable to open append %s (%s)", full_irfile, strerror(errno));
            return result;
        }
    }

    if ( strcmp(gIrCommon.ori_source, TYPE_TAP) == 0 ) {
        if ( *gszIniParGenTap[E_GEN_OUT] == 'Y' ) {
            if ( strstr(gszIniParGenTap[E_CALL_TYPE], gIrCommon.call_type) != NULL &&
                 strstr(gszIniParGenTap[E_CHARGE_TYPE], gIrCommon.chrg_type) != NULL ) {
                gen_output = TRUE;
            }
        }
    }
    else if ( strcmp(gIrCommon.ori_source, TYPE_NRT) == 0 ) {
        if ( *gszIniParGenNrt[E_GEN_OUT] == 'Y' ) {
            if ( strstr(gszIniParGenNrt[E_CALL_TYPE], gIrCommon.call_type) != NULL &&
                 strstr(gszIniParGenNrt[E_CHARGE_TYPE], gIrCommon.chrg_type) != NULL ) {
                gen_output = TRUE;
            }
        }
    }
    else {
        if ( *gszIniParGenScp[E_GEN_OUT] == 'Y' ) {
            if ( strstr(gszIniParGenScp[E_CALL_TYPE], gIrCommon.call_type) != NULL &&
                 strstr(gszIniParGenScp[E_CHARGE_TYPE], gIrCommon.chrg_type) != NULL ) {
                gen_output = TRUE;
            }
        }
    }


    if ( gen_output == TRUE ) {
        fprintf(*ofp, "%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n",
                    gIrCommon.call_type, gIrCommon.imsi, gIrCommon.st_call_date, gIrCommon.st_call_time,
                    gIrCommon.duration, gIrCommon.called_no, gIrCommon.charge, gIrCommon.pmn, gIrCommon.proc_dtm,
                    gIrCommon.volume, gIrCommon.chrg_type, gIrCommon.company_name, gIrCommon.chrg_one_tariff,
                    gIrCommon.th_st_call_dtm, gIrCommon.called_no_type, gIrCommon.risk_no_flg, gIrCommon.risk_no_id,
                    gIrCommon.billing_sys, gIrCommon.start_dtm, gIrCommon.stop_dtm, gIrCommon.chrg_id, gIrCommon.utc_time,
                    gIrCommon.total_call_evt_dur, gIrCommon.ori_rec_type, gIrCommon.mobile_no, gIrCommon.imei,
                    gIrCommon.ori_source, gIrCommon.ori_filename, gIrCommon.country_code,
                    gIrCommon.pmn_name, gIrCommon.roam_country, gIrCommon.roam_region,
                    gIrCommon.ori_duration, gIrCommon.ori_volume, gIrCommon.ori_one_charge);
        result = SUCCESS;
writeLog(LOG_DB3, "final mrg rec> stime(%s) calltype(%s) mobno(%s) imsi(%s) dur(%s) chg(%s[satang]) orichg(%s[satang]) src(%s)"
        , gIrCommon.st_call_time, gIrCommon.call_type, gIrCommon.mobile_no, gIrCommon.imsi
        , gIrCommon.duration, gIrCommon.chrg_one_tariff, gIrCommon.ori_one_charge, gIrCommon.ori_source);
    }
#if 0   // _USE_OLD_LAYOUT_
    if ( gen_output ) {

        char tmp1[21], tmp2[21], tmp3[21], tmp4[21], tmp5[21], tmp6[21], tmp7[21];
        memset(tmp1, 0x00, sizeof(tmp1)); memset(tmp2, 0x00, sizeof(tmp2)); memset(tmp3, 0x00, sizeof(tmp3));
        memset(tmp4, 0x00, sizeof(tmp4)); memset(tmp5, 0x00, sizeof(tmp5)); memset(tmp6, 0x00, sizeof(tmp6)); memset(tmp7, 0x00, sizeof(tmp7));

        // gIrCommon.st_call_date      // from YYYYMMDD to YYYY-MM-DD
        sprintf(tmp1, "%.4s-%.2s-%.2s", gIrCommon.st_call_date, gIrCommon.st_call_date+4, gIrCommon.st_call_date+6);

        // gIrCommon.proc_dtm          // from YYYYMMDDHHMMSS to YYYY-MM-DD HH:MM:SS
        sprintf(tmp2, "%.4s-%.2s-%.2s %.2s:%.2s:%.2s", gIrCommon.proc_dtm, gIrCommon.proc_dtm+4, gIrCommon.proc_dtm+6, gIrCommon.proc_dtm+8, gIrCommon.proc_dtm+10, gIrCommon.proc_dtm+12);

        // gIrCommon.chrg_one_tariff   // from satang to satang x 10 ( 1000 = 1 THB )
        sprintf(tmp3, "%ld", (atol(gIrCommon.chrg_one_tariff) * 10));

        // gIrCommon.th_st_call_dtm    // from YYYYMMDD HH:MM:SS to YYYY-MM-DD HH:MM:SS
        sprintf(tmp4, "%.4s-%.2s-%.2s %s", gIrCommon.th_st_call_dtm, gIrCommon.th_st_call_dtm+4, gIrCommon.th_st_call_dtm+6, gIrCommon.th_st_call_dtm+9);

        // gIrCommon.ori_one_charge   // from satang to satang x 10 ( 1000 = 1 THB )
        sprintf(tmp5, "%ld", (atol(gIrCommon.ori_one_charge) * 10));

        // gIrCommon.start_dtm         // from YYYYMMDD HH:MM:SS to YYYY-MM-DD HH:MM:SS
        sprintf(tmp6, "%.4s-%.2s-%.2s %s", gIrCommon.start_dtm, gIrCommon.start_dtm+4, gIrCommon.start_dtm+6, gIrCommon.start_dtm+9);

        // gIrCommon.stop_dtm         // from YYYYMMDD HH:MM:SS to YYYY-MM-DD HH:MM:SS
        if ( *gIrCommon.stop_dtm != '\0' ) {
            sprintf(tmp7, "%.4s-%.2s-%.2s %s", gIrCommon.stop_dtm, gIrCommon.stop_dtm+4, gIrCommon.stop_dtm+6, gIrCommon.stop_dtm+9);
        }

        fprintf(*ofp, "%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n",
                    gIrCommon.call_type, gIrCommon.imsi, tmp1, gIrCommon.st_call_time,
                    gIrCommon.duration, gIrCommon.mobile_no, gIrCommon.called_no, gIrCommon.charge, gIrCommon.pmn,
                    tmp2, gIrCommon.volume, gIrCommon.chrg_type, gIrCommon.company_name, tmp3,
                    tmp4, gIrCommon.called_no_type, gIrCommon.risk_no_flg, gIrCommon.risk_no_id, tmp6,
                    tmp7, gIrCommon.chrg_id, gIrCommon.utc_time, gIrCommon.ori_rec_type, gIrCommon.imei,
                    gIrCommon.pmn_name, gIrCommon.roam_country, gIrCommon.roam_region,
                    gIrCommon.ori_duration, tmp5, gIrCommon.ori_source);
        result = SUCCESS;
    }
#endif
    else {
writeLog(LOG_DB3, "skip write out common for '%s' call_type('%s', '%s') charge_type('%s', '%s')", gIrCommon.ori_source, gszIniParGenScp[E_CALL_TYPE], gIrCommon.call_type, gszIniParGenScp[E_CHARGE_TYPE], gIrCommon.chrg_type);
        result = FAILED;
    }

    return result;

}

int wrtAlrtDbConnFail(const char *odir, const char *fname, const char *dbsvr)
{
    char full_dbconfile[SIZE_ITEM_L];
    FILE *ofp = NULL;

    sprintf(full_dbconfile, "%s/%s_dbcon_%s%s", odir, gszAppName, fname, ALERT_SUFF);
    if ( (ofp = fopen(full_dbconfile, "a")) == NULL ) {
        writeLog(LOG_SYS, "unable to open append %s (%s)", full_dbconfile, strerror(errno));
        return FAILED;
    }
    fprintf(ofp, "%s %s\n", getSysDTM(DTM_DATE_TIME_FULL), dbsvr);
    fclose(ofp);
    writeLog(LOG_INF, "db connection alert file is created: %s", fname);

    return SUCCESS;

}

// int wrtOutReject(const char *odir, const char *fname, FILE *ofp, const char *record)
// {
    // char full_rejfile[SIZE_ITEM_L];
    // if ( ofp == NULL ) {
        // sprintf(full_rejfile, "%s/%s.REJ", odir, fname);
        // if ( (ofp = fopen(full_rejfile, "a")) == NULL ) {
            // writeLog(LOG_ERR, "cannot open append %s (%s)", full_rejfile, strerror(errno));
            // return FAILED;
        // }
    // }

    // fprintf(ofp, "%s\n", record);
    // return SUCCESS;

// }

void makeIni()
{

    int key;
    char cmd[SIZE_ITEM_S];
    char tmp_ini[SIZE_ITEM_S];
    char tmp_itm[SIZE_ITEM_S];

    sprintf(tmp_ini, "./%s_XXXXXX", _APP_NAME_);
    mkstemp(tmp_ini);
    strcpy(tmp_itm, "<place_holder>");

    // Write config of INPUT Section
    for ( key = 0; key < E_NOF_PAR_INPUT; key++ ) {
        ini_puts(gszIniStrSection[E_INPUT], gszIniStrInput[key], tmp_itm, tmp_ini);
    }

    // Write config of OUTPUT Section
    for ( key = 0; key < E_NOF_PAR_OUTPUT; key++ ) {
        ini_puts(gszIniStrSection[E_OUTPUT], gszIniStrOutput[key], tmp_itm, tmp_ini);
    }
    for ( key = 0; key < E_NOF_PAR_GEN; key++ ) {
        ini_puts(gszIniStrSection[E_OUT_TAP], gszIniStrGenOut[key], tmp_itm, tmp_ini);
    }
    for ( key = 0; key < E_NOF_PAR_GEN; key++ ) {
        ini_puts(gszIniStrSection[E_OUT_NRT], gszIniStrGenOut[key], tmp_itm, tmp_ini);
    }
    for ( key = 0; key < E_NOF_PAR_GEN; key++ ) {
        ini_puts(gszIniStrSection[E_OUT_SCP], gszIniStrGenOut[key], tmp_itm, tmp_ini);
    }

    // Write config of COMMON Section
    for ( key = 0; key < E_NOF_PAR_COMMON; key++ ) {
        ini_puts(gszIniStrSection[E_COMMON], gszIniStrCommon[key], tmp_itm, tmp_ini);
    }

    // Write config of BACKUP Section
    for ( key = 0; key < E_NOF_PAR_DBCONN; key++ ) {
        ini_puts(gszIniStrSection[E_DBCONN], gszIniStrDbConn[key], tmp_itm, tmp_ini);
    }

    sprintf(cmd, "mv %s %s.ini", tmp_ini, tmp_ini);
    system(cmd);
    fprintf(stderr, "ini template file is %s.ini\n", tmp_ini);

}

int chkStateAndConcat(const char *oFileName)
{
    int result = FAILED;
    DIR *p_dir;
    struct dirent *p_dirent;
    char cmd[SIZE_BUFF];
    memset(cmd, 0x00, sizeof(cmd));
    unlink(oFileName);

    if ( (p_dir = opendir(gszIniParCommon[E_STATE_DIR])) != NULL ) {
        while ( (p_dirent = readdir(p_dir)) != NULL ) {
            // state file name: <APP_NAME>_<PROC_TYPE>_YYYYMMDD.proclist
            if ( strcmp(p_dirent->d_name, ".") == 0 || strcmp(p_dirent->d_name, "..") == 0 )
                continue;

            if ( strstr(p_dirent->d_name, STATE_SUFF) != NULL &&
                 strstr(p_dirent->d_name, gszAppName) != NULL ) {
                char state_file[SIZE_ITEM_L];
                memset(state_file, 0x00, sizeof(state_file));
                sprintf(state_file, "%s/%s", gszIniParCommon[E_STATE_DIR], p_dirent->d_name);
                if ( access(state_file, F_OK|R_OK|W_OK) != SUCCESS ) {
                    writeLog(LOG_ERR, "unable to read/write file %s", state_file);
                    result = FAILED;
                    break;
                }
                else {
                    sprintf(cmd, "cat %s >> %s 2>/dev/null", state_file, oFileName);
                    system(cmd);
                    result = SUCCESS;
                }
            }
        }
        closedir(p_dir);
        return result;
    }
    else {
        return result;
    }
}

void printCommon()
{

    printf("call_type          len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.call_type)           ,(long)sizeof(gIrCommon.call_type)          ,gIrCommon.call_type         );
    printf("imsi               len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.imsi)                ,(long)sizeof(gIrCommon.imsi)               ,gIrCommon.imsi              );
    printf("st_call_date       len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.st_call_date)        ,(long)sizeof(gIrCommon.st_call_date)       ,gIrCommon.st_call_date      );
    printf("st_call_time       len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.st_call_time)        ,(long)sizeof(gIrCommon.st_call_time)       ,gIrCommon.st_call_time      );
    printf("duration           len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.duration)            ,(long)sizeof(gIrCommon.duration)           ,gIrCommon.duration          );
    printf("called_no          len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.called_no)           ,(long)sizeof(gIrCommon.called_no)          ,gIrCommon.called_no         );
    printf("charge             len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.charge)              ,(long)sizeof(gIrCommon.charge)             ,gIrCommon.charge            );
    printf("pmn                len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.pmn)                 ,(long)sizeof(gIrCommon.pmn)                ,gIrCommon.pmn               );
    printf("proc_dtm           len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.proc_dtm)            ,(long)sizeof(gIrCommon.proc_dtm)           ,gIrCommon.proc_dtm          );
    printf("volume             len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.volume)              ,(long)sizeof(gIrCommon.volume)             ,gIrCommon.volume            );
    printf("chrg_type          len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.chrg_type)           ,(long)sizeof(gIrCommon.chrg_type)          ,gIrCommon.chrg_type         );
    printf("company_name       len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.company_name)        ,(long)sizeof(gIrCommon.company_name)       ,gIrCommon.company_name      );
    printf("chrg_one_tariff    len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.chrg_one_tariff)     ,(long)sizeof(gIrCommon.chrg_one_tariff)    ,gIrCommon.chrg_one_tariff   );
    printf("th_st_call_dtm     len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.th_st_call_dtm)      ,(long)sizeof(gIrCommon.th_st_call_dtm)     ,gIrCommon.th_st_call_dtm    );
    printf("called_no_type     len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.called_no_type)      ,(long)sizeof(gIrCommon.called_no_type)     ,gIrCommon.called_no_type    );
    printf("risk_no_flg        len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.risk_no_flg)         ,(long)sizeof(gIrCommon.risk_no_flg)        ,gIrCommon.risk_no_flg       );
    printf("risk_no_id         len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.risk_no_id)          ,(long)sizeof(gIrCommon.risk_no_id)         ,gIrCommon.risk_no_id        );
    printf("billing_sys        len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.billing_sys)         ,(long)sizeof(gIrCommon.billing_sys)        ,gIrCommon.billing_sys       );
    printf("start_dtm          len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.start_dtm)           ,(long)sizeof(gIrCommon.start_dtm)          ,gIrCommon.start_dtm         );
    printf("stop_dtm           len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.stop_dtm)            ,(long)sizeof(gIrCommon.stop_dtm)           ,gIrCommon.stop_dtm          );
    printf("chrg_id            len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.chrg_id)             ,(long)sizeof(gIrCommon.chrg_id)            ,gIrCommon.chrg_id           );
    printf("utc_time           len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.utc_time)            ,(long)sizeof(gIrCommon.utc_time)           ,gIrCommon.utc_time          );
    printf("total_call_evt_dur len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.total_call_evt_dur)  ,(long)sizeof(gIrCommon.total_call_evt_dur) ,gIrCommon.total_call_evt_dur);
    printf("ori_rec_type       len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.ori_rec_type)        ,(long)sizeof(gIrCommon.ori_rec_type)       ,gIrCommon.ori_rec_type      );
    printf("mobile_no          len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.mobile_no)           ,(long)sizeof(gIrCommon.mobile_no)          ,gIrCommon.mobile_no         );
    printf("imei               len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.imei)                ,(long)sizeof(gIrCommon.imei)               ,gIrCommon.imei              );
    printf("ori_source         len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.ori_source)          ,(long)sizeof(gIrCommon.ori_source)         ,gIrCommon.ori_source        );
    printf("ori_filename       len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.ori_filename)        ,(long)sizeof(gIrCommon.ori_filename)       ,gIrCommon.ori_filename      );
    printf("country_code       len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.country_code)        ,(long)sizeof(gIrCommon.country_code)       ,gIrCommon.country_code      );
    printf("ori_duration      len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.ori_duration)       ,(long)sizeof(gIrCommon.ori_duration)      ,gIrCommon.ori_duration     );
    printf("ori_one_charge    len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.ori_one_charge)     ,(long)sizeof(gIrCommon.ori_one_charge)    ,gIrCommon.ori_one_charge   );
    printf("ori_volume        len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.ori_volume)         ,(long)sizeof(gIrCommon.ori_volume)        ,gIrCommon.ori_volume       );
    printf("pmn_name           len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.pmn_name)            ,(long)sizeof(gIrCommon.pmn_name)           ,gIrCommon.pmn_name          );
    printf("roam_country       len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.roam_country)        ,(long)sizeof(gIrCommon.roam_country)       ,gIrCommon.roam_country      );
    printf("roam_region        len='%5d' sizeof='%5ld' val='%s' \n" ,(int)strlen(gIrCommon.roam_region)         ,(long)sizeof(gIrCommon.roam_region)        ,gIrCommon.roam_region       );

}
//
// for reformat to older version
//printf("%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n", \
//$1, $2, $3, $4, $5, $25, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $19, $20, $21, $22, $24, $26,  "",  "",  "", $30, $32, $27);
//
