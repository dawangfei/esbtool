# -*- coding: UTF-8 -*-


import os,sys

from saiconf    import *
from saidb      import *
from sailog     import *
from saiutil    import *

from pub  import *


# ala, service-logic
ALA_JOB = 'DUP_ALA'
SVC_JOB = 'DUP_SVC'
NUM_JOB = 'SET_NUM'
RUT_JOB = 'IMP_RUT'
PROC_JOB = 'SVC_PROC'

# OBJECT: ala, dta
ALA_OBJ = 'A'
DTA_OBJ = 'D'

SYS_USER = 'yase'


def check_dir():

    my_dir = os.path.dirname(os.path.realpath(__file__))

    data_dir = '%s/data' % (my_dir)
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)
        log_debug('create dir: %s', data_dir)

    return 0


def check_sys_user():
    my_user = SYS_USER

    sql = "select user_name from est_user_mng_new where user_name = '%s'" % (my_user)

    MyCtx.cursorX.execute(sql)
    list1 = MyCtx.cursorX.fetchall()

    if len(list1) > 0:
        # log_debug('has user %s', my_user)
        return 0

    sql = "insert into est_user_mng_new values('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % \
           (my_user, 'sys-user', '1', my_user, '1', ' ', '20180717-123456', '20180717-123456', '111111111100000000111111100000000000000000000000000000')
    log_debug('create new user %s', my_user)
    MyCtx.cursorX.execute(sql)
    MyCtx.connX.commit()

    return 0


def dist_execute_and_record(_sql):
    MyCtx.sql_content.append(_sql)
    MyCtx.cursorX.execute(_sql)


def dist_strip_list(_list):

    for i in range(len(_list)):
        _list[i] = _list[i].strip()

    return _list


# input:  'IPAY_QRY_500, svc2, svc3'
# output: 'IPAY_QRY_500','svc2','svc3'
def dist_generate_where_in(_svc_logic_buf):
    lst = []

    for item in _svc_logic_buf.split(','):
        buf = "'" + item.strip() + "'"
        lst.append(buf)

    buf = ','.join(lst)

    return buf



def dist_get_relation_id(_ala_name, _cursor):

    if len(_ala_name) == 0:
        return ''

    sql  = "select project_id + bus_id + sub_bus_id rel_id, sub_bus_name from est_sub_bus where sub_bus_name = '%s'" % (_ala_name)
    # log_debug('%s', sql)

    _cursor.execute(sql)
    list1 = _cursor.fetchall()

    rel_id = ''

    row = list1[0]
    rel_id  = row['rel_id']
    log_debug('rel_id: %s', rel_id)

    return rel_id


def dist_generate_res_id(_table, _cursor):

    gen_newid_list_map = {
        'est_sub_bus'       : ['sub_bus_id',    '19'],
        'est_svc_logic'     : ['service_id',    '10'],
        'est_event_expr'    : ['event_expr_id', '28'],
    }


    my_key = '%s' % (_table)

    if not gen_newid_list_map.has_key(my_key):
        return '', ''

    val_list    = gen_newid_list_map[my_key]

    id_name = val_list[0]  # sub_bus_id
    id_type = val_list[1]  # '19'

    new_res_id = my_get_next_id(id_type, _cursor)

    # log_debug('ID generated: [%s.%s] => [%s]', _table, id_name, new_res_id)

    return id_name, new_res_id


def dist_convert_res_id(_table):

    cvt_resid_list_map = {
        'est_svc_logic_revs'    : ['svc_logic_id'],
        'est_svc_proc'          : ['service_id'],
    }

    my_key = '%s' % (_table)

    if not cvt_resid_list_map.has_key(my_key):
        return '', ''

    val_list    = cvt_resid_list_map[my_key]

    id_col = val_list[0]  # svc_logic_id

    new_res_id = MyCtx.new_svc_id

    log_debug('ID converted: [%s.%s] => [%s]', _table, id_col, new_res_id)

    return id_col, new_res_id


def dist_generate_suffix(_seq):
    suffix = '%02d%s' % (_seq, MyCtx.user_defined)
    return suffix

def dist_generate_ala_name(_ala_name, _seq):
    ala_name = '%s_%s' % (_ala_name, dist_generate_suffix(_seq))
    return ala_name

def dist_generate_svc_name(_svc_name, _seq):
    svc_name = '%s_%s' % (_svc_name, dist_generate_suffix(_seq))
    return svc_name

def dist_generate_res_name(_table, _seq):

    gen_newname_list_map = {
        'est_sub_bus'   : 'sub_bus_name',
        'est_svc_logic' : 'svc_name',
    }


    my_key = '%s' % (_table)

    if not gen_newname_list_map.has_key(my_key):
        return '', ''

    col_name = gen_newname_list_map[my_key]

    suffix = dist_generate_suffix(_seq)

    return col_name, suffix


def dist_generate_res_desc(_table, _seq):

    gen_newdesc_list_map = {
        'est_sub_bus'   : 'sub_bus_desc',
    }
    #   'est_svc_logic' : 'svc_desc',

    my_key = '%s' % (_table)

    if not gen_newdesc_list_map.has_key(my_key):
        return '', ''

    col_name = gen_newdesc_list_map[my_key]

    suffix = dist_generate_suffix(_seq)

    return col_name, suffix



def dist_get_project_id(_project_name):
    sql = "select project_id from est_project where project_name='%s'" % (_project_name)
    MyCtx.cursorX.execute(sql)

    list1 = MyCtx.cursorX.fetchall()

    # only 1 line actually
    project_id  = ''
    for row in list1:
        project_id  = row['project_id'].strip()
        log_debug('project: %s => %s', _project_name, project_id)

    return project_id


def dist_get_env_id(_env_name):
    sql = "select env_id from est_env_mng where env_name='%s'" % (_env_name)
    MyCtx.cursorX.execute(sql)

    list1 = MyCtx.cursorX.fetchall()

    # only 1 line actually
    env_id  = ''
    for row in list1:
        env_id  = row['env_id'].strip()
        log_debug('env: %s => %s', _env_name, env_id)

    return env_id


def dist_get_mchn_id(_mchn_name):
    sql = "select mchn_id from est_mchn_mng where mchn_name='%s'" % (_mchn_name)
    MyCtx.cursorX.execute(sql)

    list1 = MyCtx.cursorX.fetchall()

    # only 1 line actually
    mchn_id  = ''
    for row in list1:
        mchn_id  = row['mchn_id'].strip()
        log_debug('mchn: %s => %s', _mchn_name, mchn_id)

    return mchn_id


def dist_get_dta_id(_dta_name):
    sql = "select dta_id from est_dta where dta_name='%s'" % (_dta_name)
    MyCtx.cursorX.execute(sql)

    list1 = MyCtx.cursorX.fetchall()

    # only 1 line actually
    dta_id  = ''
    for row in list1:
        dta_id  = row['dta_id'].strip()
        log_debug('dta: %s => %s', _dta_name, dta_id)

    return dta_id


def dist_get_ala_id(_ala_name):
    sql = "select sub_bus_id from est_sub_bus where sub_bus_name='%s'" % (_ala_name)
    log_debug('%s', sql)
    MyCtx.cursorX.execute(sql)

    list1 = MyCtx.cursorX.fetchall()

    # only 1 line actually
    ala_id  = ''
    for row in list1:
        ala_id  = row['sub_bus_id'].strip()
        log_debug('ala: %s => %s', _ala_name, ala_id)

    return ala_id


def dist_get_svc_id(_svc_name):
    sql = "select service_id from est_svc_logic where svc_name='%s'" % (_svc_name)
    log_debug('%s', sql)
    MyCtx.cursorX.execute(sql)

    list1 = MyCtx.cursorX.fetchall()

    # only 1 line actually
    svc_id  = ''
    for row in list1:
        svc_id  = row['service_id'].strip()
        log_debug('svc: %s => %s', _svc_name, svc_id)

    return svc_id



def dist_get_next_serial_no(_dta_id, _rule_id):
    sql = "select isnull(max(serial_no),0)+1 next_id from est_route_entrance where src_dta_id='%s' and rule_id='%s'" % (_dta_id, _rule_id)
    log_debug('%s', sql)
    MyCtx.cursorX.execute(sql)

    list1 = MyCtx.cursorX.fetchall()

    # only 1 line actually
    next_id  = ''
    for row in list1:
        next_id  = str(row['next_id'])
        log_debug('serial: %s, %s => %s', _dta_id, _rule_id, next_id)

    return next_id


def dist_svc_belongs_ala(_svc_name, _ala_name):
    ala_relation_id = ''
    svc_relation_id = ''

    # ALA relation-id
    sql = "select project_id+bus_id+sub_bus_id relation_id from est_sub_bus where sub_bus_name='%s'" % (_ala_name)
    log_debug('%s', sql)
    MyCtx.cursorX.execute(sql)
    list1 = MyCtx.cursorX.fetchall()
    # only 1 line actually
    for row in list1:
        ala_relation_id = str(row['relation_id'])
        log_debug('belong-ala: %s => %s', _ala_name, ala_relation_id)

    if len(ala_relation_id) == 0:
        log_error('error: invalid ALA: %s', _ala_name)
        return False


    # SVC relation-id
    sql = "select relation_id from est_svc_logic where svc_name='%s'" % (_svc_name)
    log_debug('%s', sql)
    MyCtx.cursorX.execute(sql)
    list1 = MyCtx.cursorX.fetchall()
    # only 1 line actually
    for row in list1:
        svc_relation_id = str(row['relation_id'])
        log_debug('belong-svc: %s => %s', _ala_name, ala_relation_id)

    if len(svc_relation_id) == 0:
        log_error('error: invalid SVC: %s', _svc_name)
        return False

    if ala_relation_id != svc_relation_id:
        log_error('error: invalid belong: svc(%s, %s) -- ala(%s, %s)', _svc_name, svc_relation_id, _ala_name, ala_relation_id)
        return False

    return True


def dist_get_rule_id(_dta_id, _rule_name):
    sql = "select rule_id from est_route_rule where src_dta_id='%s' and rule_name='%s'" % (_dta_id, _rule_name)
    MyCtx.cursorX.execute(sql)

    list1 = MyCtx.cursorX.fetchall()

    # only 1 line actually
    rule_id  = ''
    for row in list1:
        rule_id  = row['rule_id'].strip()
        log_debug('rule: %s => %s', _rule_name, rule_id)

    return rule_id


def dist_check_ala_exist(_ala_name, _start, _last):
    for i in range(_start, _last+1):
        ala_name = dist_generate_ala_name(_ala_name, i)
        log_debug('PRE-check ala -- %d: %s', i, ala_name)
        sql = "select sub_bus_name from est_sub_bus where sub_bus_name = '%s'" % (ala_name)
        MyCtx.cursorX.execute(sql)
        list1 = MyCtx.cursorX.fetchall()
        if len(list1) <= 0:
            log_error('error: ala [%s] not exist', ala_name)
            return -1
    return 0



def dist_check_svc_exist(_svc_names):
    svc_list = _svc_names.split(',')
    for svc_name in svc_list:
        svc_name = svc_name.strip()
        log_debug('PRE-check svc -- [%s]', svc_name)
        sql = "select service_id from est_svc_logic where svc_name = '%s'" % (svc_name)
        MyCtx.cursorX.execute(sql)
        list1 = MyCtx.cursorX.fetchall()
        if len(list1) <= 0:
            log_error('error: svc [%s] not exist', svc_name)
            return -1
    return 0


def dist_update_date_time(_row):

    curr_date_time = sai_get_date_time()

    my_key = 'crt_date_time'
    if _row.has_key(my_key):
        _row[my_key] = curr_date_time


    my_key = 'lst_mod_date_time'
    if _row.has_key(my_key):
        _row[my_key] = curr_date_time

    return 0


def dist_generate_event_one(_event_id):

    table = 'est_event_expr'

    sql_list = []

    ###################################################################
    # DB/mem -- get table structure
    if len(MyCtx.event_list) == 0:
        log_info('query table structure')
        sql   = 'sp_columns %s' % (table)
        MyCtx.cursorX.execute(sql)
        list1 = MyCtx.cursorX.fetchall()
        MyCtx.event_list = list1
    else:
        list1 = MyCtx.event_list
        log_info('get table structure')

    ###################################################################
    # DB -- get source table's data
    key_column = 'event_expr_id'
    sql   = "select * from %s where %s = '%s'" % (table, key_column , _event_id)
    MyCtx.cursorX.execute(sql)
    list2 = MyCtx.cursorX.fetchall()
    if len(list2) <= 0:
        log_info('warn: event no data: %s', sql)
        return '', []

    ###################################################################
    # DB -- generate new resource-id
    event_col, new_event_id = dist_generate_res_id(table, MyCtx.cursorX)
    if len(new_event_id) <= 0:
        log_info('error: no new event id: %s', table)
        raise Exception


    ###################################################################
    for data_row in list2:

        log_debug('CHANGE-event: [%s][%s] [%s] => [%s]', event_col, data_row['serial_no'],  data_row[event_col], new_event_id)
        data_row[event_col] = new_event_id

        dist_update_date_time(data_row)

        column_name_list    = []
        column_value_list   = []
        for row in list1:
            column_name = row['COLUMN_NAME']
            column_type = row['TYPE_NAME']

            if data_row[column_name] is None:
                column_value= "null"
            else:
                column_value= "'" + str(data_row[column_name]).strip() + "'"

            if column_type == 'int identity':
                # log_debug('%s -- %s', column_name, column_type)
                continue

            column_name_list.append(column_name)
            column_value_list.append(column_value)
            # log_debug('[%s] => [%s]', column_name, column_value)

        buf1 = ', '.join(column_name_list)

        buf2 = ', '.join(column_value_list)

        sql = "insert into %s (%s) values (%s)" % (table, buf1, buf2)
        sql_list.append(sql)
        log_debug('\n%s', sql)


    return new_event_id, sql_list



def dist_generate_event(_table, _row):

    gen_newevent_list_map = {
        'est_sub_bus'   : ['start_init_proc', 'svc_pre_proc', 'svc_succ_proc', 'svc_fail_proc', 'term_proc', 'revs_init', 'revs_cond'],
        'est_svc_logic' : ['svc_succ_proc', 'svc_lost_proc', 'svc_pre_proc', 'revs_init'],
        'est_svc_proc'  : ['bef_proc', 'aft_proc', 'fail_proc', 'step_proc', 'comps_init_proc', 'comps_end_proc'],
    }

    my_key = '%s' % (_table)

    if not gen_newevent_list_map.has_key(my_key):
        return 1

    val_list = gen_newevent_list_map[my_key]

    for item in val_list:
        cur_event_id = _row[item]


        if cur_event_id is None or len(cur_event_id) == 0:
            continue

        cur_event_id = str(cur_event_id)

        if len(cur_event_id) == 5 and cur_event_id.isdigit():
            log_debug('%s ==> %s ==> %s', _table, item, cur_event_id)

            new_event_id, sql = dist_generate_event_one(cur_event_id)
            if len(new_event_id) <= 0:
                log_info('warn: event-expr(%s) exists ever, but already cleared. [%s.%s => %s]', cur_event_id, _table, item, cur_event_id)

            _row[item] = new_event_id

            for one in sql:
                dist_execute_and_record(one)


            log_debug('event-id: %s ==> [%s] ==> [%s]', item, cur_event_id, new_event_id)
        else:
            log_debug('ignore Event-id: [%s]', cur_event_id)

    return 0



def dist_generate_insert_cm(_table, _key_value, _seq):
    log_debug('-'*80)
    table_key_column_map = {
        'est_sub_bus'           : 'sub_bus_name',
        'est_svc_logic'         : 'svc_name',
        'est_svc_logic_revs'    : 'svc_logic_id',
        'est_svc_proc'          : 'service_id',
    }

    sql_list = []

    ###################################################################
    # SQL -- get table structure
    sql   = 'sp_columns %s' % (_table)
    MyCtx.cursorX.execute(sql)
    list1 = MyCtx.cursorX.fetchall()

    ###################################################################
    # SQL -- get source table's data
    key_column = table_key_column_map[_table]
    sql   = "select * from %s where %s = '%s'" % (_table, key_column , _key_value)
    MyCtx.cursorX.execute(sql)
    list2 = MyCtx.cursorX.fetchall()
    if len(list2) <= 0:
        log_info('warn: no data: %s', sql)
        return []
    #  data_row = list2[0]

    ###################################################################
    for data_row in list2:

        # use system user
        if data_row.has_key('crt_user'):
            data_row['crt_user'] = SYS_USER
        if data_row.has_key('lst_mod_user'):
            data_row['lst_mod_user'] = SYS_USER

        # use new relation-id
        if data_row.has_key('relation_id'):
            new_rel_id = dist_get_relation_id(MyCtx.new_ala_name, MyCtx.cursorX)
            if len(new_rel_id) > 0:
                old_rel_id = data_row['relation_id']
                log_debug('CHANGE-relation: [%s] => [%s]', old_rel_id, new_rel_id)
                data_row['relation_id'] = new_rel_id

        # generate new name
        res_name_col, res_name_suffix = dist_generate_res_name(_table, _seq)
        if len(res_name_suffix) > 0:
            old_res_name = data_row[res_name_col].strip()
            new_res_name = "%s_%s" % (old_res_name, res_name_suffix)
            log_debug('CHANGE-name: [%s] => [%s]', old_res_name, new_res_name)
            data_row[res_name_col] = new_res_name

            if _table == 'est_sub_bus':
                MyCtx.new_ala_name = new_res_name
                log_debug('new ala-name: %s', MyCtx.new_ala_name)
            elif _table == 'est_svc_logic':
                MyCtx.new_svc_name = new_res_name
                # log_debug('new svc-name: %s', MyCtx.new_svc_name)
            else:
                log_debug('xxx')


        # generate new desc
        res_name_col, suffix = dist_generate_res_desc(_table, _seq)
        if len(suffix) > 0:
            old_res_name = data_row[res_name_col].strip()
            new_res_name = "%s_%s" % (old_res_name, suffix)
            log_debug('CHANGE-name: [%s] => [%s]', old_res_name, new_res_name)
            data_row[res_name_col] = new_res_name


        # generate new resource-id
        res_id_col, new_res_id = dist_generate_res_id(_table, MyCtx.cursorX)
        if len(new_res_id) > 0:
            log_debug('CHANGE-id: [%s] => [%s]', data_row[res_id_col], new_res_id)
            data_row[res_id_col] = new_res_id

            if _table == 'est_svc_logic':
                MyCtx.new_svc_id = new_res_id
                log_debug('new svc-id: %s', MyCtx.new_svc_id)



        # convert resource-id
        res_id_col, cvt_res_id = dist_convert_res_id(_table)
        if len(cvt_res_id) > 0:
            log_debug('CHANGE-id2: [%s] => [%s]', data_row[res_id_col], cvt_res_id)
            data_row[res_id_col] = cvt_res_id

        # convert event-id
        rv = dist_generate_event(_table, data_row)
        if rv < 0:
            log_error('error: dist_generate_event: %s', _table)
            return []

        # update date time
        dist_update_date_time(data_row)

        column_name_list    = []
        column_value_list   = []
        for row in list1:
            column_name = row['COLUMN_NAME']
            column_type = row['TYPE_NAME']

            if data_row[column_name] is None:
                column_value= "null"
            else:
                column_value= "'" + str(data_row[column_name]).strip() + "'"

            if column_type == 'int identity':
                # log_debug('%s -- %s', column_name, column_type)
                continue

            column_name_list.append(column_name)
            column_value_list.append(column_value)
            # log_debug('[%s] => [%s]', column_name, column_value)

        buf1 = ', '.join(column_name_list)

        buf2 = ', '.join(column_value_list)

        sql = "insert into %s (%s) values (%s)" % (_table, buf1, buf2)
        sql_list.append(sql)
        log_debug('\n%s', sql)

    return sql_list



def dist_generate_one(_ala_name, _svc_name, _svc_id, _seq):


    ###################################################################
    # service logic
    table_name = 'est_svc_logic'
    sql = dist_generate_insert_cm(table_name, _svc_name, _seq)
    # log_debug('%s', sql)

    for one in sql:
        dist_execute_and_record(one)

    ###################################################################
    # sub-table-1
    table_name = 'est_svc_logic_revs'
    sql = dist_generate_insert_cm(table_name, _svc_id, _seq)
    # log_debug('%s', sql)
    for one in sql:
        dist_execute_and_record(one)

    ###################################################################
    # sub-table-2
    table_name = 'est_svc_proc'
    sql = dist_generate_insert_cm(table_name, _svc_id, _seq)
    # log_debug('%s', sql)
    for one in sql:
        dist_execute_and_record(one)


    return 0



def dist_duplicate_sub_bus(_ala_name, _seq):

    ###################################################################
    sql  = "select project_id + bus_id + sub_bus_id rel_id, sub_bus_name from est_sub_bus where sub_bus_name = '%s'" % (_ala_name)

    log_debug('%s', sql)

    MyCtx.cursorX.execute(sql)

    list1 = MyCtx.cursorX.fetchall()

    # only 1 line actually
    rel_id  = ''
    ala_name= ''
    for row in list1:
        rel_id  = row['rel_id']
        ala_name= row['sub_bus_name']
        log_debug('rel_id: %s, name: %s', rel_id, ala_name)

    if len(rel_id) == 0:
        print("error: not found ALA: '%s'" % _ala_name)
        return -1


    # do generate
    table_name = 'est_sub_bus'
    MyCtx.new_ala_name = ''
    sql = dist_generate_insert_cm(table_name, _ala_name, _seq)
    # log_debug('%s', sql)

    for one in sql:
        dist_execute_and_record(one)

    ###################################################################

    sql = "select * from est_svc_logic where relation_id = '%s'" % (rel_id)
    # log_debug('%s', sql)

    MyCtx.cursorX.execute(sql)

    list2 = MyCtx.cursorX.fetchall()

    # more than 1 usually
    for row in list2:
        svc_id  = row['service_id']
        svc_name= row['svc_name']
        # log_debug('%s -- %s', svc_id, svc_name)
        MyCtx.new_svc_name = ''
        MyCtx.new_svc_id   = ''
        dist_generate_one(ala_name, svc_name, svc_id, _seq)

    ###################################################################


    return 0



def dist_duplicate_svc_logic(_ala_name, _svc_logics, _seq):

    ###################################################################
    sql  = "select project_id + bus_id + sub_bus_id rel_id, sub_bus_name from est_sub_bus where sub_bus_name = '%s'" % (_ala_name)

    log_debug('%s', sql)

    MyCtx.cursorX.execute(sql)

    list1 = MyCtx.cursorX.fetchall()

    # only 1 line actually
    rel_id  = ''
    ala_name= ''
    for row in list1:
        rel_id  = row['rel_id']
        ala_name= row['sub_bus_name'].strip()
        log_debug('rel_id: %s, name: %s', rel_id, ala_name)

    if len(rel_id) == 0:
        print("error: not found ALA: '%s'" % _ala_name)
        return -1

    ###################################################################
    MyCtx.new_ala_name = '%s_%s' % (ala_name, dist_generate_suffix(_seq))

    svc_logic = dist_generate_where_in(_svc_logics)
    sql = "select * from est_svc_logic where svc_name in (%s)" % (svc_logic)
    log_debug('svc: %s', sql)

    MyCtx.cursorX.execute(sql)

    list2 = MyCtx.cursorX.fetchall()

    # more than 1 usually
    for row in list2:
        svc_id  = row['service_id']
        svc_name= row['svc_name']

        log_debug('svc: %s -- %s', svc_id, svc_name)
        MyCtx.new_svc_name = ''
        MyCtx.new_svc_id   = ''
        dist_generate_one(ala_name, svc_name, svc_id, _seq)

    ###################################################################


    return 0


def dist_set_object_num(_obj, _seq, _inst_num, _inst_max):

    obj = '%s_%s' % (_obj, dist_generate_suffix(_seq))
    obj_type = 'A'
    ###################################################################
    # get object-id
    log_debug('ala get object-id')
    sql = "select sub_bus_id obj_id from est_sub_bus where sub_bus_name='%s'" % (obj)


    log_debug('%s', sql)
    MyCtx.cursorX.execute(sql)
    list1 = MyCtx.cursorX.fetchall()
    # only 1 line actually
    obj_id  = ''
    for row in list1:
        obj_id  = row['obj_id'].strip()
        log_debug('object-id: %s => %s', obj, obj_id)

    if len(obj_id) == 0:
        log_error('error: not found object: %s', obj)
        return -1

    project_id = MyCtx.project_id
    env_id     = MyCtx.env_id
    mchn_id    = MyCtx.mchn_id

    sql  = "select * from est_dta_mchn_mng where obj_id='%s' and type='%s' and project_id='%s' and env_id='%s' and mchn_id='%s'" % (obj_id, obj_type, project_id, env_id, mchn_id)
    log_debug('query: %s', sql)
    MyCtx.cursorX.execute(sql)

    list2 = MyCtx.cursorX.fetchall()

    log_debug('dta-mchn rs rows: %d', len(list2))
    #log_debug('%s', list2)

    if len(list2) == 0:
        # new, let's insert
        log_debug('its new, lets insert')
        sql = "insert into est_dta_mchn_mng (obj_id, type, project_id, env_id, mchn_id, inst_num, max_inst, cmd_parm) values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (obj_id, obj_type, project_id, env_id, mchn_id, _inst_num, _inst_max, '')
    elif len(list2) == 1:
        # update, let's update
        log_debug('already exist, lets update')
        sql = "update est_dta_mchn_mng set inst_num='%s', max_inst='%s' where obj_id='%s' and type='%s' and project_id='%s' and env_id='%s' and mchn_id='%s'" % (_inst_num, _inst_max, obj_id, obj_type, project_id, env_id, mchn_id)
    else:
        log_error('error: invalid mchn query rs: %d', len(list2))
        return -1

    log_debug('execute: %s', sql)
    dist_execute_and_record(sql)

    return 0


def dist_import_route_one(_list):

    rv = 0

    sql = ''

    src_dta_id = MyCtx.dta_id
    rule_id    = MyCtx.rule_id

    serial_no  = '' # 0, 1, 2 ... n
    dst_ala_id = ''
    dst_svc_id = ''


    action = _list[0]
    match  = _list[1]

    log_debug('action: %s', action)

    if action == 'ADD':
        dst_ala = _list[2]
        dst_svc = _list[3]
        desc    = _list[4]
        log_debug('ADD: [%s] => [%s, %s, %s]', match, dst_ala, dst_svc, desc)

        serial_no   = dist_get_next_serial_no(src_dta_id, rule_id)
        if len(serial_no) == 0:
            log_error('error: dist_get_next_serial_no: %s, %s', src_dta_id, rule_id)
            return -1

        dst_ala_id  = dist_get_ala_id(dst_ala)
        if len(dst_ala_id) == 0:
            log_error('error: dist_get_ala_id: %s', dst_ala)
            print('error: ALA(%s) not exist' % dst_ala)
            return -1

        dst_svc_id  = dist_get_svc_id(dst_svc)
        if len(dst_svc_id) == 0:
            log_error('error: dist_get_svc_id: %s', dst_svc)
            print('error: SVC(%s) not exist' % dst_svc)
            return -1

        if not dist_svc_belongs_ala(dst_svc, dst_ala):
            log_error('error: dist_svc_belongs_ala: %s, %s', dst_svc, dst_ala)
            print('error: SVC(%s) not belongs ALA(%s)' % (dst_svc, dst_ala))
            return -1

        sql = "insert into est_route_entrance (src_dta_id, rule_id, serial_no, entrance_desc, match_expression, dest_dta_id, dest_type, svc_id, node_id, resp_flag, node_svc_name) values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (src_dta_id, rule_id, serial_no, desc, match, dst_ala_id, 'ALA', dst_svc_id, '', 'Y', '')
        log_debug('%s', sql)
    elif action == 'DEL':
        log_error('DEL: dont support')
        print('error: dont support DEL')
        return -1
    else:
        log_error('DEL: invalid action: %s', action)
        return -1

    if len(sql) > 0:
        dist_execute_and_record(sql)

    return 0



def dist_import_route_file(_route_file):
    rv = 0

    fo = open(_route_file, "r")

    for line in fo.readlines():
        line = line.strip()
        log_debug('-' * 80)

        if len(line) == 0 or line[0] == '#':
            log_debug('ignore this line: %s', line)
            continue

        elem_list = line.split(',')
        dist_strip_list(elem_list)
        rv = dist_import_route_one(elem_list)
        if rv < 0:
            log_error('error: dist_import_route_one')
            break

    fo.close()

    return rv


def dist_svc_proc_get_column(_input):
    column = ''

    col_map = {
        'BEFORE_PROC':'svc_pre_proc',
        'SUCC_PROC':'svc_succ_proc',
        'FAIL_PROC':'svc_lost_proc'
    }

    if col_map.has_key(_input):
        column = col_map[_input]


    return column



def dist_svc_proc_one_cm_insert(_exprs, _proc_id):
    sql_list = []

    table = 'est_event_expr'

    buf1 = 'event_expr_id, event_expr_type, serial_no, event_expr_proc, event_expr_desc, comp_id'

    expr_list = _exprs.split('|')
    dist_strip_list(expr_list)

    for seq in range(len(expr_list)):
        expr = str(expr_list[seq])
        expr = expr.replace("'", "''")
        if len(expr) == 0:
            break
        serial_no = seq + 1

        log_debug('event: [%d] => [%s]', seq, expr)

        buf2 = "'%s', '%s', '%d', '%s', '%s', '%s'" % (_proc_id, '0', serial_no, expr, '', '')

        sql = "insert into %s (%s) values (%s)" % (table, buf1, buf2)
        sql_list.append(sql)

    return sql_list


def dist_svc_proc_one_new(_exprs, _column, _svc_name):
    log_debug('new-exprs:   %s', _exprs)
    log_debug('new-column:  %s', _column)
    log_debug('new-svc:     %s', _svc_name)

    sql_list = []
    table = 'est_event_expr'

    ###################################################################
    # STEP1: generate new resource-id
    event_col, proc_id = dist_generate_res_id(table, MyCtx.cursorX)
    if len(proc_id) <= 0:
        log_info('error: no new event id: %s', table)
        raise Exception

    # STEP2: insert new
    insert_list = dist_svc_proc_one_cm_insert(_exprs, proc_id)
    sql_list    += insert_list

    # STEP3: update to meta column
    table = 'est_svc_logic'
    curr_date_time = sai_get_date_time()
    curr_user = SYS_USER
    sql = "update %s set %s='%s', lst_mod_date_time='%s', lst_mod_user='%s' where svc_name = '%s'" % \
    (table, _column, proc_id, curr_date_time, curr_user, _svc_name)
    sql_list.append(sql)

    # for one in sql_list:
    #     log_debug('%s', one)

    return sql_list


def dist_svc_proc_one_old(_exprs, _proc_id):
    sql_list = []

    log_debug('old-exprs:   %s', _exprs)
    log_debug('old-procid:  %s', _proc_id)


    table = 'est_event_expr'

    # STEP1: delete old
    sql = "delete from %s where event_expr_id = '%s'" % (table, _proc_id)
    # log_debug('old-delete: %s', sql)
    sql_list.append(sql)

    # STEP2: insert new
    insert_list = dist_svc_proc_one_cm_insert(_exprs, _proc_id)
    sql_list    += insert_list

    # for one in sql_list:
    #     log_debug('%s', one)

    return sql_list


def dist_svc_proc_one(_list):

    rv = 0

    sql = ''

    src_dta_id = MyCtx.dta_id
    rule_id    = MyCtx.rule_id

    serial_no  = '' # 0, 1, 2 ... n
    dst_ala_id = ''
    dst_svc_id = ''
    sql_list   = []

    if len(_list) < 3:
        log_error('error: invalid input: %d', len(_list))
        return -1

    action      = _list[0]
    svc_name    = _list[1]
    proc_type   = _list[2]

    column = dist_svc_proc_get_column(proc_type)
    if len(column) == 0:
        log_debug('error: invalid type %s', proc_type)
        print('error: invalid type: %s' % proc_type)
        return -1

    log_debug('action: %s - %s - %s - %s', action, svc_name, proc_type, column)

    # get PROC-id
    sql = "select %s proc_id from est_svc_logic where svc_name='%s'" % (column, svc_name)
    MyCtx.cursorX.execute(sql)
    list1 = MyCtx.cursorX.fetchall()
    proc_id  = ''
    for row in list1:
        proc_id = row['proc_id'].strip()
        log_debug('proc: %s => %s', column, proc_id)

    if action == 'MOD':
        exprs   = _list[3]
        log_debug('MOD: [%s] => [%s]', svc_name, exprs)

        if len(proc_id) == 0:
            # not exist
            log_debug('proc-id not exist, create it')
            # 1: generate a new proc-id
            # 2: insert into expr
            # 3: update to record
            sql_list = dist_svc_proc_one_new(exprs, column, svc_name)
        else:
            # exist
            log_debug('proc-id already exist, delete and insert: %s', proc_id)
            # 1: delete all data under this proc-id
            # 2: insert into expr
            sql_list = dist_svc_proc_one_old(exprs, proc_id)
        log_debug('MOD process done: %d', len(sql_list))
    elif action == 'DEL':
        log_debug('DEL: [%s] => [%s]', svc_name, column)
        if len(proc_id) > 0:
            sql = "delete from est_event_expr where event_expr_id = '%s'" % (proc_id)
            sql_list.append(sql)
        else:
            log_error('error: event not exist: %s - %s', svc_name, proc_type)
            return -1
    else:
        log_error('invalid action: %s', action)
        return -1

    if len(sql_list) > 0:
        for sql in sql_list:
            log_debug('proc-sql: %s', sql)
            dist_execute_and_record(sql)

    return 0


def dist_svc_proc_file(_action_file):
    rv = 0

    fo = open(_action_file, "r")

    for line in fo.readlines():
        line = line.strip()
        log_debug('-' * 80)

        if len(line) == 0 or line[0] == '#':
            log_debug('ignore this line: %s', line)
            continue

        elem_list = line.split(',,,')
        dist_strip_list(elem_list)
        rv = dist_svc_proc_one(elem_list)
        if rv < 0:
            log_error('error: dist_svc_proc_one')
            break

    fo.close()
    log_debug('-' * 80)
    log_debug('-' * 80)
    log_debug('-' * 80)

    return rv


def dist_save(_sql_list):
    file_name = "sql.%s.txt" % (sai_get_date_time())
    file_path = '%s/data/%s' % (os.path.dirname(os.path.realpath(__file__)), file_name)
    charset   = 'GB18030'

    fo = open(file_path, "w")

    for item in _sql_list:
        fo.write(item.encode(charset)+'\n')

    fo.close()

    return 0




def dist_dup_ala(_ala_name):

    log_debug('dist_dup_ala: --- RES and LOG ---')

    start_str   = sai_conf_get("DUP_ALA", "START")
    last_str    = sai_conf_get("DUP_ALA", "LAST")

    start = int(start_str)
    last  = int(last_str)

    if start > last:
        print('error: invalid config: start[%d] > last[%d]', start, last)
        return -1

    print('will generate the following ALA')
    for seq in range(start, last+1):
        print('-- %s_%s --' % (_ala_name, dist_generate_suffix(seq)))
    raw_input("Press any to continue")

    for seq in range(start, last+1):
        log_debug('seq: %d', seq)
        rv = dist_duplicate_sub_bus(_ala_name, seq)
        if rv < 0:
            print("error: dist_duplicate_sub_bus: '%s, %s'" % (_ala_name, seq))
            return -1

    dist_save(MyCtx.sql_content)
    MyCtx.connX.commit()

    print('nice: import %d sql succeeds' % len(MyCtx.sql_content))

    return 0


def dist_dup_svc(_ala_name, _svc_logics):

    log_debug('dist_dup_svc: --- RES and LOG ---')

    start_str   = sai_conf_get("DUP_SVC", "START")
    last_str    = sai_conf_get("DUP_SVC", "LAST")

    start = int(start_str)
    last  = int(last_str)

    if start > last:
        print('error: invalid config: start[%d] > last[%d]', start, last)
        return -1

    print('will modify the following ALA')
    for seq in range(start, last+1):
        print('-- %s_%s --' % (_ala_name, dist_generate_suffix(seq)))
    raw_input("Press any to continue") # TODO

    # ala-exist: 'TPP_QRY'
    if dist_check_ala_exist(_ala_name, start, last) < 0:
        log_error('error: dist_check_ala_exist')
        return -1

    # svc-exist: 'IPAY_QRY_500, IPAY_QRY_501'
    if dist_check_svc_exist(_svc_logics) < 0:
        log_error('error: dist_check_svc_exist')
        return -1

    for seq in range(start, last+1):
        log_debug('seq: %d', seq)
        rv = dist_duplicate_svc_logic(_ala_name, _svc_logics, seq)
        if rv < 0:
            print("error: dist_duplicate_svc_logic: '%s, %s, %d'" % (_ala_name, _svc_logics, seq))
            return -1

    dist_save(MyCtx.sql_content)
    MyCtx.connX.commit()

    print('nice: import %d sql succeeds' % len(MyCtx.sql_content))

    return 0


def dist_set_num():

    log_debug('dist_set_num: --- RES and LOG ---')

    obj_name    = sai_conf_get("SET_NUM", "NAME")
    inst_num    = sai_conf_get("SET_NUM", "INST_NUM")
    inst_max    = sai_conf_get("SET_NUM", "INST_MAX")
    project     = sai_conf_get("SET_NUM", "PROJECT")
    env         = sai_conf_get("SET_NUM", "ENVIRONMENT")
    mchn        = sai_conf_get("SET_NUM", "MACHINE")

    start_str   = sai_conf_get("SET_NUM", "START")
    last_str    = sai_conf_get("SET_NUM", "LAST")

    start = int(start_str)
    last  = int(last_str)

    if start > last:
        print('error: invalid config: start[%d] > last[%d]', start, last)
        return -1

    print('will modify the following ALA')
    for seq in range(start, last+1):
        print('-- %s_%s --' % (obj_name, dist_generate_suffix(seq)))
    raw_input("Press any to continue") # TODO

    # ala-exist: 'TPP_QRY'
    if dist_check_ala_exist(obj_name, start, last) < 0:
        log_error('error: dist_check_ala_exist')
        return -1

    # project-name => project-id
    project_id = dist_get_project_id(project)
    if len(project_id) == 0:
        log_error('error: dist_get_project_id')
        return -1
    MyCtx.project_name = project
    MyCtx.project_id   = project_id

    # environment-name => environment-id
    env_id = dist_get_env_id(env)
    if len(env_id) == 0:
        log_error('error: dist_get_env_id')
        return -1
    MyCtx.env_name = env
    MyCtx.env_id   = env_id

    # machine-name => machine-id
    mchn_id = dist_get_mchn_id(mchn)
    if len(mchn_id) == 0:
        log_error('error: dist_get_mchn_id')
        return -1
    MyCtx.mchn_name = mchn
    MyCtx.mchn_id   = mchn_id

    for seq in range(start, last+1):
        log_debug('-' * 80)
        log_debug('seq: %d', seq)
        rv = dist_set_object_num(obj_name, seq, inst_num, inst_max)
        if rv < 0:
            log_error("error: dist_set_object_num: '%s, %s, %d'", obj_name, seq)
            return -1

    dist_save(MyCtx.sql_content)
    MyCtx.connX.commit()

    print('nice: update %d sql succeeds' % len(MyCtx.sql_content))

    return 0


def dist_import_route():

    log_debug('dist_import_route: --- RES and LOG ---')

    dta_name    = sai_conf_get(RUT_JOB, "DTA_NAME")
    route_name  = sai_conf_get(RUT_JOB, "ROUTE_NAME")
    route_file  = sai_conf_get(RUT_JOB, "ROUTE_FILE")
    project     = sai_conf_get(RUT_JOB, "PROJECT")

    log_debug('route-file: %s', route_file)


    # get src DTA-id
    dta_id = dist_get_dta_id(dta_name)
    if len(dta_id) == 0:
        log_error('error: dist_get_dta_id')
        return -1
    MyCtx.dta_name  = dta_name
    MyCtx.dta_id    = dta_id

    # get RULE-id
    rule_id = dist_get_rule_id(dta_id, route_name)
    if len(rule_id) == 0:
        log_error('error: dist_get_rule_id')
        return -1
    MyCtx.rule_name = route_name
    MyCtx.rule_id   = rule_id


    rv = dist_import_route_file(route_file)
    if rv < 0:
        log_error('error: dist_import_route_file')
        return rv

    dist_save(MyCtx.sql_content)
    MyCtx.connX.commit()

    print('nice: insert %d sql succeeds' % len(MyCtx.sql_content))

    return 0


def dist_set_proc():

    log_debug('dist_set_proc: --- RES and LOG ---')

    action_file  = sai_conf_get(PROC_JOB, "PROC_FILE")
    log_debug('proc-action-file: %s', action_file)

    rv = dist_svc_proc_file(action_file)
    if rv < 0:
        log_error('error: dist_svc_proc_file')
        return rv
    log_debug('set-svc-proc process file done')

    dist_save(MyCtx.sql_content)
    MyCtx.connX.commit()
    log_debug('set-svc-proc commit done')

    print('nice: execute %d sql succeeds' % len(MyCtx.sql_content))

    return 0


# 
def dist_init():

    args = sai_get_args()

    job  = ''
    if len(args) == 1:
        job = args[0]
        if job == ALA_JOB:
            pass
        elif job == SVC_JOB:
            pass
        elif job == NUM_JOB:
            pass
        elif job == RUT_JOB:
            pass
        elif job == PROC_JOB:
            pass
        else:
            log_error('error: invalid job: [%s]', job)
            print('error: invalid job: [%s]' % job)
            job = ''
    else:
        print('error: not support: %s' % args)
        #job = ALA_JOB
        #job = NUM_JOB
        #job = SVC_JOB
        #job = RUT_JOB
        #job = PROC_JOB

    if len(job) == 0:
        print('only [%s, %s, %s, %s, %s] available' % (ALA_JOB, SVC_JOB, NUM_JOB, RUT_JOB, PROC_JOB))
        print('please try: dist.exe %s' % ALA_JOB)
        print('            dist.exe %s' % SVC_JOB)
        print('            dist.exe %s' % NUM_JOB)
        print('            dist.exe %s' % RUT_JOB)
        print('            dist.exe %s' % PROC_JOB)
        return ''

    conf_file = 'my.conf'
    MyConf.conf_path = conf_file

    MyCtx.input_job = job

    if job == SVC_JOB or job == ALA_JOB or job == NUM_JOB:
        MyCtx.input_ala_name = sai_conf_get(job, 'ALA_NAME')
        log_debug('ala-name: %s', MyCtx.input_ala_name)

    if job == SVC_JOB:
        MyCtx.input_svc_name = sai_conf_get(job, 'SVC_NAME')
        log_debug('svc-name: %s', MyCtx.input_db_name)

    MyCtx.input_db_name = sai_conf_get(job, 'DB_NAME')
    log_debug('db-name: %s', MyCtx.input_db_name)

    MyCtx.user_defined  = sai_conf_get(job, 'SUFFIX')
    log_debug('user-suffix: [%s]', MyCtx.user_defined)

    # DB connection
    MyCtx.connX  = db_init(MyCtx.input_db_name)
    MyCtx.cursorX= MyCtx.connX.cursor(as_dict=True)

    check_sys_user()

    check_dir()

    return job


def dist_done():
    # finally
    if MyCtx.connX is not None:
        MyCtx.connX.rollback()
        MyCtx.connX.close()

    return 0


def dist_doit(_job):
    rv = 0

    ala_name = ''
    svc_name = ''

    if _job == ALA_JOB:
        ala_name = MyCtx.input_ala_name
        log_debug('to duplicate ALA: [%s]', ala_name)
        rv = dist_dup_ala(ala_name)
    elif _job == SVC_JOB:
        ala_name = MyCtx.input_ala_name
        svc_name = MyCtx.input_svc_name
        log_debug('to duplicate SVC: [%s, %s]', ala_name, svc_name)
        rv = dist_dup_svc(ala_name, svc_name)
    elif _job == NUM_JOB:
        log_debug('to set number')
        rv = dist_set_num()
    elif _job == RUT_JOB:
        log_debug('to import route')
        rv = dist_import_route()
    elif _job == PROC_JOB:
        log_debug('to set_proc')
        rv = dist_set_proc()
    else:
        log_error('error: invalid job: %s', _job)
        rv = -1

    return rv


def dist_run():
    rv = 0

    job = dist_init()
    if len(job) == 0:
        print('error: init failure')
        return -1

    rv = dist_doit(job)
    if rv < 0:
        print('error: failure, please see log')
    #dist_test()

    dist_done()

    return rv


def dist_test():
    rv = 0

    ala_name = 'TPP_QRY'
    start = 1
    last  = 4
    cursor = MyCtx.cursorX
    if dist_check_ala_exist(ala_name, start, last, cursor) < 0:
        log_error('error: dist_check_ala_exist')

    svc_logics = 'IPAY_QRY_500, IPAY_QRY_501'
    if dist_check_svc_exist(svc_logics, cursor) < 0:
        log_error('error: dist_check_svc_exist')

    return rv


# main()
if __name__=="__main__":
    sailog_set("run.log")
    dist_run()
    #dist_test()



# dist.py
