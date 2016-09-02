#!/usr/bin/env python
# -*- coding: utf-8 -*-
#__author__ = 'o.sidorov', 2014

import sys
import datetime
import MySQLdb as db
import math


def sql_query(args, time, code, interval, napr, src, dst, origId, termId, route_num):
    sql_comm = 'SELECT cdr.cdr_date, cdr.in_ani,' \
           ' cdr.out_ani, cdr.src_name, cdr.in_dnis,' \
           ' cdr.out_dnis, cdr.dst_name, elapsed_time,' \
           ' CASE when c.code_h323 is not null then c.code_h323 else c.default_code_h323 end as code_h323,' \
           ' cdr.disconnect_initiator, c.reason, last_cdr' \
           ' FROM mvts_cdr cdr LEFT OUTER JOIN mvts_code c ON cdr.disconnect_code = c.ucode' \
           ' WHERE cdr.cdr_date > (utc_timestamp() - interval %d MINUTE)' % time

    if len(interval) != 0:
        if len(interval) == 2 and interval[0] < interval[1]:
            if interval[0] == 0:
                sql_comm += ' AND (elapsed_time is NULL OR (elapsed_time >= \'%d\' AND elapsed_time < \'%d\'))' % ((interval[1] - 1) * 1000, interval[1] * 1000)
            else:
                sql_comm += ' AND ('
                for x in range(0, len(interval)):
                    if x == 0:
                        sql_comm += 'elapsed_time >=\'%d\' ' % ((interval[x] * 1000) - 999)
                    else:
                        sql_comm += 'AND elapsed_time <=\'%d\'' % (interval[x] * 1000)
                sql_comm += ') '

        elif len(interval) == 1:
            if interval[0] == 0:
                sql_comm += 'AND elapsed_time is NULL '
            else:
                sql_comm += 'AND (elapsed_time >= \'%d\' AND elapsed_time < \'%d\') ' % ((interval[0] - 1) * 1000, interval[0] * 1000)
        else:
            print 'Error. Recording format command : ED-TIME=X-Y, where x - beginning of the period , y - end of the period , and x <y'
            print 'Try \'aon_hour ?\' for more information.'
            sys.exit(1)

    if len(napr) != 0:
        sql_comm += 'AND ('
        for x in range(0, len(napr)):
            if x != 0:
                sql_comm += ' OR bill_ani LIKE \'%s%s\'' % ('%#%#%#______________', napr[x])
            else:
                sql_comm += 'bill_ani LIKE \'%s%s\'' % ('%#%#%#______________', napr[x])
        sql_comm += ') '

    if len(src) != 0:
        sql_comm += 'AND ('
        for x in range(0, len(src)):
            if x != 0:
                sql_comm += ' OR src_name LIKE \'%s%s%s\'' % ('%', src[x], '%')
            else:
                sql_comm += 'src_name LIKE \'%s%s%s\'' % ('%', src[x], '%')
        sql_comm += ') '

    if len(dst) != 0:
        sql_comm += 'AND ('
        for x in range(0, len(dst)):
            if x != 0:
                sql_comm += ' OR dst_name LIKE \'%s%s%s\'' % ('%', dst[x], '%')
            else:
                sql_comm += 'dst_name LIKE \'%s%s%s\'' % ('%', dst[x], '%')
        sql_comm += ') '

    if len(origId) != 0:
        sql_comm += 'AND ('
        for x in range(0, len(origId)):
            if x != 0:
                sql_comm += ' OR bill_ani LIKE \'%s%s%s\'' % ('%#', origId[x], '#%#%')
            else:
                sql_comm += 'bill_ani LIKE \'%s%s%s\'' % ('%#', origId[x], '#%#%')
        sql_comm += ') '

    if len(termId) != 0:
        sql_comm += 'AND ('
        for x in range(0, len(termId)):
            if x != 0:
                sql_comm += ' OR bill_ani LIKE \'%s%s%s\'' % ('%#%#', termId[x], '#%')
            else:
                sql_comm += 'bill_ani LIKE \'%s%s%s\'' % ('%#%#', termId[x], '#%')
        sql_comm += ') '

    if len(route_num) != 0:
        sql_comm += 'AND ('
        for x in range(0, len(route_num)):
            if x != 0:
                sql_comm += ' OR bill_ani LIKE \'%s%s%s\'' % ('%*', route_num[x], '#%#%#%')
            else:
                sql_comm += 'bill_ani LIKE \'%s%s%s\'' % ('%*', route_num[x], '#%#%#%')
        sql_comm += ') '

    if len(code) != 0:
        sql_comm += 'AND ('
        for x in range(0, len(code)):
            if x != 0:
                sql_comm += ' OR c.default_code_h323 =\'%s\'' % code[x]
            else:
                sql_comm += ' c.default_code_h323 =\'%s\'' % code[x]
        sql_comm += ') '

    if args == 0:
        return sql_comm
    else:
        sql_param = ['in_ani', 'out_ani', 'src_name', 'in_dnis', 'out_dnis', 'dst_name', 'remote_src_sig_address', 'remote_dst_sig_address']
        for x in range(0, len(args)):
            sql_comm += 'AND ('
            for y in range(0, len(sql_param)):
                if y != 0:
                    sql_comm += ' OR ' + sql_param[y] + ' LIKE \'%' + args[x] + '%\''
                else:
                    sql_comm += sql_param[y] + ' LIKE \'%' + args[x] + '%\''
            sql_comm += ') '
        return sql_comm

def time_change(time):
    GTM_Moscow = datetime.timedelta(hours=3)
    changeTime = (time + GTM_Moscow).timetz()
    return str(changeTime)

def ms_to_sec(t):
    return int(math.ceil(t/1000.0))

def cdr_pring(cdr):
    cdr_time = time_change(cdr[0])
    in_ani = cdr[1]
    out_ani = cdr[2]
    src_name = cdr[3]
    in_dnis = cdr[4]
    out_dnis = cdr[5]
    dst_name = cdr[6]
    if cdr[7] is not None:
        elapsed_time = str(int(ms_to_sec(cdr[7])))
    else:
        elapsed_time = '0'
    dc_H323 = str(cdr[8]) + '(' + str(cdr[9]) + ')'
    reason = cdr[10]
    print '%5s %20s %20s %25s %20s %25s %25s %9s %10s %35s' % (cdr_time, in_ani, out_ani, src_name, in_dnis, out_dnis, dst_name, elapsed_time, dc_H323, reason)

def main():
    interval = 60     # default time (60 min)
    total_dur = 0
    total_call_true = 0
    total_call_answer = 0
    total_call = 0
    list_code = []
    interval_time = []
    napr_code = []
    src_name = []
    dst_name = []
    orig_id = []
    term_id = []
    route_num = []
    args = sys.argv[1:]


    if not args:
        print 'There are no arguments , please enter the data to search for.'
        print 'Try \'aon_hour ?\' for more information.'
        sys.exit(1)

    elif len(args) > 9:
        print ('Too many arguments... ')
        print 'Try \'aon_hour ?\' for more information.'
        sys.exit(1)

    elif args[0] == '?':
        print '/last update 22.10.2014/'
        print '%s' % ('List of commands:')
        print ' --last [min] [args]                 if the parameter [min] is empty, the script displays the values in the last hour'
        print '                                     if the parameter [args] is empty, the script will output values without filter'
        print ' 931=[param1,param2,...,paramX]      To find log by exit code uses the key \'931=\' with parameters through \',\' ( together, separated by commas )'
        print ' ED-TIME=X-Y                         where \'X\' - beginning of the period , \'Y\' - end of the period , and \'X\' < \'Y\''
        print ' ED-TIME=X                           where \'X\' - call duration'
        print ' napr=[param1,param2,...,paramX]     To find log by code call direction uses the key \'napr=\' with parameters through \',\' ( together, separated by commas )'
        print ' SRC-NAME=[param1,param2,...,paramX] To find log by SRC-NAME uses the key \'SRC-NAME=\' with parameters through \',\' ( together, separated by commas )'
        print ' DST-NAME=[param1,param2,...,paramX] To find log by DST-NAME uses the key \'DST-NAME=\' with parameters through \',\' ( together, separated by commas )'
        print ' orig-id=[param1,param2,...,paramX]  To find log by ID ORIGINATOR uses the key \'orig-id=\' with parameters through \',\' ( together, separated by commas )'
        print ' term-id=[param1,param2,...,paramX]  To find log by ID TERMINATOR uses the key \'term-id=\' with parameters through \',\' ( together, separated by commas )'
        print ' route=X                             To find log by choice TERMINATOR uses the key \'route=\''
        print '                                     where \'X\' - selection number on the route'
        sys.exit(1)

    elif args[0] == '--last':
        if len(args) > 1:
            try:
                interval = int(args[1])
            except ValueError:
                print '\'%s\' - this parameter should only contain numbers' % args[1]
                print 'Try \'aon_hour ?\' for more information.'
                sys.exit(1)
            if len(args) == 2:
                args = 0
            else:
                args = args[2:]
        else:
            args = 0
			
	# connection information
    conn = db.connect(host = '',
                  user = '',
                  passwd = '',
                  db = '')
    cur = conn.cursor()

    if args != 0:
        code_remove = []
        for num in range(len(args)):
            if '931=' in args[num]:
                find_code = args[num][4:].split(',')
                code_remove.append(args[num])
                for x in find_code:
                    try:
                        y = int(x)
                        list_code.append(y)
                    except:
                        print '931=\'%s\' - error. Option to filter \'931\' should be a number' % x
                        print 'Try \'aon_hour ?\' for more information.'
                        sys.exit(1)

            if 'ED-TIME=' in args[num] or 'ed-time=' in args[num]:
                edTime = args[num][8:].split('-')
                code_remove.append(args[num])
                for x in edTime:
                    try:
                        y = int(x)
                        interval_time.append(y)
                    except:
                        print 'ED-TIME=\'%s\' - error. Option to filter \'ED-TIME\' should be a number (integer)' % x
                        print 'Try \'aon_hour ?\' for more information.'
                        sys.exit(1)

            if 'napr=' in args[num] or 'NAPR=' in args[num]:
                naprCode = args[num][5:].split(',')
                code_remove.append(args[num])
                for x in naprCode:
                    try:
                        y = int(x)
                        napr_code.append(y)
                    except:
                        print 'napr=\'%s\' - error. Option to filter \'napr\' should be a number (integer)' % x
                        print 'Try \'aon_hour ?\' for more information.'
                        sys.exit(1)

            if 'SRC-NAME=' in args[num] or 'src-name' in args[num]:
                srcName = args[num][9:].split(',')
                code_remove.append(args[num])
                for x in range(0, len(srcName)):
                    try:
                        src_name.append(srcName[x])
                    except:
                        print 'error srcName'
                        sys.exit(1)

            if 'DST-NAME=' in args[num] or 'dst-name=' in args[num]:
                dstName = args[num][9:].split(',')
                code_remove.append(args[num])
                for x in range(0, len(dstName)):
                    try:
                        dst_name.append(dstName[x])
                    except:
                        print 'error dstName'
                        sys.exit(1)

            if 'ORIG-ID' in args[num] or 'orig-id' in args[num]:
                origID = args[num][8:].split(',')
                code_remove.append(args[num])
                for x in origID:
                    try:
                        y = int(x)
                        orig_id.append(y)
                    except:
                        print 'orig-id=\'%s\' - error. Option to filter \'orig-id\' should be a number (integer)' % x
                        print 'Try \'aon_hour ?\' for more information.'
                        sys.exit(1)

            if 'TERM-ID' in args[num] or 'term-id' in args[num]:
                termID = args[num][8:].split(',')
                code_remove.append(args[num])
                for x in termID:
                    try:
                        y = int(x)
                        term_id.append(y)
                    except:
                        print 'term-id=\'%s\' - error. Option to filter \'term-id\' should be a number (integer)' % x
                        print 'Try \'aon_hour ?\' for more information.'
                        sys.exit(1)

            if 'ROUTE=' in args[num] or 'route=' in args[num]:
                routeNum = args[num][6:].split(',')
                code_remove.append(args[num])
                for x in routeNum:
                    try:
                        y = int(x)
                        print y
                        route_num.append(y)
                    except:
                        print 'route=\'%s\' - error. Option to filter \'route\' should be a number (integer)' % x
                        print 'Try \'aon_hour ?\' for more information.'
                        sys.exit(1)

        if len(code_remove) != 0:
            for x in range(0, len(code_remove)):
                args.remove(code_remove[x])

    sql_select = sql_query(args, interval, list_code, interval_time, napr_code, src_name, dst_name, orig_id, term_id, route_num)
    cur.execute(sql_select)
    numrows = int(cur.rowcount)
    print '%5s %20s %20s %20s %25s %25s %20s %17s %10s %20s' % ('TIME', 'IN ANI', 'OUT ANI', 'SRC NAME', 'IN DNIS', 'OUT DNIS', 'DST NAME', 'DUR', 'H323_DC', 'REASON')
    print '%s' % '-' * 206

    for x in range(0, numrows):
        row = cur.fetchone()
        if row[7] is not None:
            total_dur += int(ms_to_sec(row[7]))
        if (str(row[8]) == '16' and str(row[9]) == '1') or (str(row[8]) == '17' and str(row[9]) == '2') or (str(row[8]) == '16' and str(row[9]) == '2' and row[7] is not None):
            total_call_true += 1
        if row[7] > 0:
            total_call_answer += 1
        if row[11] == 1:
            total_call += 1
        cdr_pring(row)
    cur.close()
    conn.close()

    if total_call_answer != 0:
        ACD_first = (total_dur / total_call_answer) / 60
        ACD_last = (total_dur / total_call_answer) % 60
    else:
        ACD_first = 0
        ACD_last = 0

    if numrows != 0 and total_call_true != 0:
        ASR = (float(total_call_true) / total_call) * 100
    else:
        ASR = 0

    print '%s' % '-' * 206
    print 'Total calls: %d | Total answer calls: %d | Total fail calls: %d' % (total_call, total_call_answer, total_call - total_call_true)
    print 'ACD = %d.%d | ASR = %.2f%% | Total duration of calls : %ds(%dm%ds)' % (ACD_first, ACD_last, ASR, total_dur, total_dur / 60, total_dur % 60)

if __name__ == '__main__':
    main()