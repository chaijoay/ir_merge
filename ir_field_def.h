///
///
/// FACILITY    : input and output field definition of IR cdr.
///
/// FILE NAME   : ir_field_def.h
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
///
///
#ifndef __IR_FIELD_DEF_H__
#define __IR_FIELD_DEF_H__

#ifdef  __cplusplus
    extern "C" {
#endif

// output structure; record layout
typedef struct _ir_common_ {
    char call_type[3];              // 01: 18 - GPRS, 20 - Voice MO, 21 - SMS MO, 25 - Voice MO CF, 22 - Call Forward, 26 - Call Forward CF, 45 - Call Forward CF, 30 - Voice MT, 31 - SMS MT
    char imsi[16];                  // 02: IMSI
    char st_call_date[9];           // 03: Start Call Date (format YYYYMMDD)
    char st_call_time[7];           // 04: Start Call Time (format HHMMSS)
    char duration[21];              // 05: recalculated-duration (differentiated after merging) - This becomes actual usage field for FRM
    char called_no[26];             // 06: Calling Number / Called Number / APN
    char charge[21];                // 07: Charge amount + VAT +Markup  unit in "SDR" (decimal point is 3 position  ex. 256.789 => 256789)
    char pmn[11];                   // 08: PMN Code
    char proc_dtm[15];              // 09: Start Call Date (format YYYYMMDDHH24MISS)
    char volume[21];                // 10: recalculated-volume (differentiated after merging)   - This becomes actual usage field for FRM
    char chrg_type[2];              // 11: 1 - Postpaid, 0 - Prepaid
    char company_name[4];           // 12: AIS, AWN
    char chrg_one_tariff[21];       // 13: recalculated-charge (differentiated after merging)   - This becomes actual usage field for FRM
    char th_st_call_dtm[18];        // 14: Thai date time (home)  (format yyyymmdd hh24:MM:ss)
    char called_no_type[6];         // 15: Called number flag (Thai, Local, IDD)
    char risk_no_flg[2];            // 16: Called number is risk number (Y - Risk, N - Normal)
    char risk_no_id[21];            // 17: Risk Number Pattern ID (running seq in db table)
    char billing_sys[5];            // 18: Billing System Flag (BOS, IRB, RTB, AVT, INS)
    char start_dtm[18];             // 19: Call Start date time  ( format yyyymmdd hh24:MM:ss )
    char stop_dtm[18];              // 20: Call Stop date time  ( format yyyymmdd hh24:MM:ss )
    char chrg_id[11];               // 21: Charging Id
    char utc_time[6];               // 22: UTC offset
    char total_call_evt_dur[21];    // 23: Call duration in second
    char ori_rec_type[21];          // 24: Original Record Type (Event Type ID-Sub Event Type ID-Modifier) (TAP)
    char mobile_no[21];             // 25: Mobile Number (Ano)
    char imei[21];                  // 26: IMEI
    char ori_source[4];             // 27: TAP, NRT, SCP
    char ori_filename[100];         // 28: Original input file name
    char country_code[5];           // 29: Country code
    char ori_duration[21];         // 30: Original Record Duration of call in second
    char ori_volume[21];           // 31: Original Record Volume (Byte)
    char ori_one_charge[21];       // 32: Original Record Charge amount calculate from One Tariff unit in 'Satang'
    char pmn_name[50];              // 33: PMN Name
    char roam_country[50];          // 34: Roaming Country
    char roam_region[50];           // 35: Roaming Region
    time_t start_dtm_time;          // 36: TEMP field for insert db
} ST_IR_COMMON;

// input record layout
typedef enum {
    E_CALLTYPE = 0,              // 00: key1
    E_IMSI,                      // 01: key2
    E_ST_CALL_DATE,              // 02
    E_ST_CALL_TIME,              // 03
    E_DURATION,                  // 04: calculation field
    E_CALLED_NO,                 // 05: key3
    E_CHARGE,                    // 06
    E_PMN,                       // 07
    E_PROC_DTM,                  // 08
    E_VOLUME,                    // 09: calculation field
    E_CHRG_TYPE,                 // 10: key4
    E_COMPANY_NAME,              // 11
    E_CHRG_ONE_TARIFF,           // 12: calculation field
    E_TH_ST_CALL_DTM,            // 13
    E_CALLED_NO_TYPE,            // 14
    E_RISK_NO_FLG,               // 15
    E_RISK_NO_ID,                // 16
    E_BILLING_SYS,               // 17
    E_START_DTM,                 // 18: conditional field
    E_STOP_DTM,                  // 19
    E_CHRG_ID,                   // 20
    E_UTC_TIME,                  // 21
    E_TOTAL_EVT_DUR,             // 22
    E_ORI_REC_TYPE,              // 23
    E_MOBILE_NO,                 // 24
    E_IMEI,                      // 25
    E_ORI_SOURCE,                // 26: conditional field
    E_ORI_FILENAME,              // 27
    E_COUNTRY_CODE,              // 28
    E_PMN_NAME,                  // 29
    E_ROAM_COUNTRY,              // 30
    E_ROAM_REGION,               // 31
    NOF_IR_FLD                   // 32
} E_IR_FLD;

#ifdef  __cplusplus
    }
#endif

#endif
