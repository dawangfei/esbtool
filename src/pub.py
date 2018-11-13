# -*- coding: UTF-8 -*-

import os

from saiconf    import *
from saidb      import *
from sailog     import *
from saiutil    import *


class MyCtx():


    # src-database
    connX  = None
    cursorX= None


    sql_content = []

    seperator   = '+' * 72

    ##################################################################
    input_db_name  = ''
    input_ala_name = ''
    input_svc_name = ''
    input_job      = ''

    project_name   = ''
    env_name       = ''
    mchn_name      = ''
    project_id     = ''
    env_id         = ''
    mchn_id        = ''
    ##################################################################
    new_ala_name = ''
    new_svc_name = ''
    new_svc_id   = ''
    event_list   = []
    ##################################################################


    ####################################################### export  ##
    ##################################################################



    # convert RELATION-ID
    rel_conv_list_map = {
        'est_element'       : ['relation_id'],
        'est_func'          : ['relation_id'],
        'est_enum'          : ['relation_id'],
        'est_format'        : ['relation_id'],
        'est_component'     : ['relation_id'],
    }


    # generate NEW res-id
    res_gen_list_map = {
        'est_element'       : ['elem_id'],
        'est_func'          : ['func_id'],
        'est_enum'          : ['enum_id'],
        'est_format'        : ['fmt_id'],
        'est_flow'          : ['flow_id'],
        'est_component'     : ['comp_id'],
    }


    id_mng_map  = {
        'elem_id'   :   '16',
        'func_id'   :   '08',
        'enum_id'   :   '25',
        'fmt_id'    :   '03',
        'flow_id'   :   '11',
        'comp_id'   :   '26',
    }


    # convert res-id for foreign-key dependant
    # list_map + rule_map work together
    res_cvt_list_map = {
        'est_element_log'       : ['elem_id'],

        'est_func_log'          : ['func_id'],
        'est_func_param'        : ['func_id'],
        'est_func_param_log'    : ['func_id'],

        'est_enum'              : ['enum_fmem_id', 'enum_smem_id'],
        'est_enum_log'          : ['enum_id', 'enum_fmem_id', 'enum_smem_id'],
        'est_enum_value'        : ['enum_id'],
        'est_enum_value_log'    : ['enum_id'],

        'est_format_log'        : ['fmt_id'],
        'est_fmt_item'          : ['fmt_id', 'elem_id'],
        'est_sign_item'         : ['fmt_id', 'elem_id'],
        'est_fmt_item_log'      : ['fmt_id', 'elem_id'],
        'est_sign_item_log'     : ['fmt_id', 'elem_id'],

        # flow
        'est_flow'              : ['retcode_elem', 'retmsg_elem'],
        'est_flow_log'          : ['flow_id', 'retcode_elem', 'retmsg_elem'],
        'est_flow_step'         : ['flow_id'],
        'est_flow_step_log'     : ['flow_id'],
        'est_flw_end'           : ['flow_id'],
        'est_flw_end_log'       : ['flow_id'],
        'est_flw_process'       : ['flow_id'],
        'est_flw_process_log'   : ['flow_id'],
        'est_flw_component'     : ['flow_id', 'comp_expr_id'],
        'est_flw_component_log' : ['flow_id', 'comp_expr_id'],

        # component
        'est_component_log'     : ['comp_id'],
        'est_comp_param'        : ['comp_id', 'func_id'],
        'est_comp_in_out'       : ['comp_id', 'elem_id'],
        'est_comp_nesting'      : ['comp_id', 'comp_nesting_id', 'source_elem_id', 'dest_elem_id'],
        'est_comp_param_log'    : ['comp_id', 'func_id'],
        'est_comp_in_out_log'   : ['comp_id', 'elem_id'],
        'est_comp_nesting_log'  : ['comp_id', 'comp_nesting_id', 'source_elem_id', 'dest_elem_id'],


    }


    res_cvt_rule_map = {
        'est_element_log-elem_id'               : ['est_element',   'elem_id',  'elem_name'],

        'est_func_log-func_id'                  : ['est_func',      'func_id',  'func_name'],
        'est_func_param-func_id'                : ['est_func',      'func_id',  'func_name'],
        'est_func_param_log-func_id'            : ['est_func',      'func_id',  'func_name'],

        'est_enum-enum_fmem_id'                 : ['est_element',   'elem_id',  'elem_name'],
        'est_enum-enum_smem_id'                 : ['est_element',   'elem_id',  'elem_name'],
        'est_enum_log-enum_id'                  : ['est_enum',      'enum_id',  'enum_name'],
        'est_enum_log-enum_fmem_id'             : ['est_element',   'elem_id',  'elem_name'],
        'est_enum_log-enum_smem_id'             : ['est_element',   'elem_id',  'elem_name'],
        'est_enum_value-enum_id'                : ['est_enum',      'enum_id',  'enum_name'],
        'est_enum_value_log-enum_id'            : ['est_enum',      'enum_id',  'enum_name'],

        # format
        'est_format_log-fmt_id'                 : ['est_format',    'fmt_id',   'fmt_name'],
        'est_fmt_item-fmt_id'                   : ['est_format',    'fmt_id',   'fmt_name'],
        'est_fmt_item-elem_id'                  : ['est_element',   'elem_id',  'elem_name'],
        'est_sign_item-fmt_id'                  : ['est_format',    'fmt_id',   'fmt_name'],
        'est_sign_item-elem_id'                 : ['est_element',   'elem_id',  'elem_name'],
        'est_fmt_item_log-fmt_id'               : ['est_format',    'fmt_id',   'fmt_name'],
        'est_fmt_item_log-elem_id'              : ['est_element',   'elem_id',  'elem_name'],
        'est_sign_item_log-fmt_id'              : ['est_format',    'fmt_id',   'fmt_name'],
        'est_sign_item_log-elem_id'             : ['est_element',   'elem_id',  'elem_name'],

        # flow
        'est_flow_step-GET_RES_ID'              : ['est_flow',      'flow_id',  'flow_name'],
        'est_flw_end-GET_RES_ID'                : ['est_flow',      'flow_id',  'flow_name'],

        'est_flow-retcode_elem'                 : ['est_element',   'elem_id',  'elem_name'],
        'est_flow-retmsg_elem'                  : ['est_element',   'elem_id',  'elem_name'],
        'est_flow_log-flow_id'                  : ['est_flow',      'flow_id',  'flow_name'],
        'est_flow_log-retcode_elem'             : ['est_element',   'elem_id',  'elem_name'],
        'est_flow_log-retmsg_elem'              : ['est_element',   'elem_id',  'elem_name'],
        'est_flow_step-flow_id'                 : ['est_flow',      'flow_id',  'flow_name'],
        'est_flow_step_log-flow_id'             : ['est_flow',      'flow_id',  'flow_name'],
        'est_flw_end-flow_id'                   : ['est_flow',      'flow_id',  'flow_name'],
        'est_flw_end_log-flow_id'               : ['est_flow',      'flow_id',  'flow_name'],
        'est_flw_process-flow_id'               : ['est_flow',      'flow_id',  'flow_name'],
        'est_flw_process_log-flow_id'           : ['est_flow',      'flow_id',  'flow_name'],
        'est_flw_component-flow_id'             : ['est_flow',      'flow_id',  'flow_name'],
        'est_flw_component-comp_expr_id'        : ['est_component', 'comp_id',  'comp_name'], 
        'est_flw_component_log-flow_id'         : ['est_flow',      'flow_id',  'flow_name'],
        'est_flw_component_log-comp_expr_id'    : ['est_component', 'comp_id',  'comp_name'],

        # component
        'est_component_log-comp_id'             : ['est_component', 'comp_id',  'comp_name'],
        'est_comp_param-comp_id'                : ['est_component', 'comp_id',  'comp_name'],
        'est_comp_param-func_id'                : ['est_func',      'func_id',  'func_name'],
        'est_comp_in_out-comp_id'               : ['est_component', 'comp_id',  'comp_name'],
        'est_comp_in_out-elem_id'               : ['est_element',   'elem_id',  'elem_name'],
        'est_comp_param_log-comp_id'            : ['est_component', 'comp_id',  'comp_name'],
        'est_comp_param_log-func_id'            : ['est_func',      'func_id',  'func_name'],
        'est_comp_in_out_log-comp_id'           : ['est_component', 'comp_id',  'comp_name'],
        'est_comp_in_out_log-elem_id'           : ['est_element',   'elem_id',  'elem_name'],
        'est_comp_nesting-comp_id'              : ['est_component', 'comp_id',  'comp_name'],
        'est_comp_nesting-comp_nesting_id'      : ['est_component', 'comp_id',  'comp_name'],
        'est_comp_nesting-source_elem_id'       : ['est_element',   'elem_id',  'elem_name'],
        'est_comp_nesting-dest_elem_id'         : ['est_element',   'elem_id',  'elem_name'],
        'est_comp_nesting_log-comp_id'          : ['est_component', 'comp_id',  'comp_name'],
        'est_comp_nesting_log-comp_nesting_id'  : ['est_component', 'comp_id',  'comp_name'],
        'est_comp_nesting_log-source_elem_id'   : ['est_element',   'elem_id',  'elem_name'],
        'est_comp_nesting_log-dest_elem_id'     : ['est_element',   'elem_id',  'elem_name'],
    }



    def __init__(self):
        print('inited')


def trim_list(_list):

    for i in range(len(_list)):
        _list[i] = _list[i].strip()

    return _list



my_meta_id = '00000'
my_fail_id = '!@#$%'


def my_is_meta_id(_id):
    global my_meta_id
    return _id == my_meta_id


def parse_relation_id(_relation_id):
    # log_debug('%s', _relation_id)

    project_id = _relation_id[0:5]
    bus_id     = _relation_id[5:10]
    sub_bus_id = _relation_id[10:15]

    # log_debug('project    -- %s', project_id)
    # log_debug('bus_id     -- %s', bus_id)
    # log_debug('sub_bus_id -- %s', sub_bus_id)

    return project_id, bus_id, sub_bus_id


def get_target_project_id(_relation_id, _src_cursor, _tgt_cursor):
    global my_meta_id
    global my_fail_id

    project_id, bus_id, sub_id = parse_relation_id(_relation_id)

    if my_is_meta_id(project_id):
        # log_debug('project-id is meta: %s', project_id)
        return  project_id

    sql = "select project_name from est_project where project_id='%s'" % (project_id)

    _src_cursor.execute(sql)
    list1 = _src_cursor.fetchall()

    if len(list1) <= 0:
        log_error('error: no data: %s', sql)
        return my_fail_id

    row = list1[0]
    project_name = row['project_name'].strip()
    # log_debug('src - project_name -- [%s]', project_name)

    #  get target project-name from config file 2018-5-14
    project_name = MyCtx.target_project
    # log_debug('tgt - project_name -- [%s]', project_name)


    sql = "select project_id from est_project where project_name='%s'" % (project_name)

    _tgt_cursor.execute(sql)
    list1 = _tgt_cursor.fetchall()

    if len(list1) <= 0:
        log_error('error: no data: %s', sql)
        return my_fail_id

    row = list1[0]
    project_id = row['project_id']
    # log_debug('tgt - project_id   -- %s', project_id)

    return project_id


def get_target_bus_id(_src_relation_id, _tgt_relation_id, _src_cursor, _tgt_cursor):
    global my_meta_id
    global my_fail_id

    project_id, bus_id, sub_id = parse_relation_id(_src_relation_id)

    if my_is_meta_id(bus_id):
        # log_debug('bus-id is meta: %s', bus_id)
        return bus_id

    sql = "select bus_name from est_bus where project_id = '%s' and  bus_id='%s'" % (project_id, bus_id)
    _src_cursor.execute(sql)
    list1 = _src_cursor.fetchall()

    if len(list1) <= 0:
        log_error('error: no data: %s', sql)
        return my_fail_id

    row = list1[0]
    bus_name = row['bus_name'].strip()
    # log_debug('src - bus_name -- %s', bus_name)


    project_id2, bus_id2, sub_id2 = parse_relation_id(_tgt_relation_id)
    sql = "select bus_id from est_bus where project_id = '%s' and bus_name='%s'" % (project_id2, bus_name)
    _tgt_cursor.execute(sql)
    list1 = _tgt_cursor.fetchall()

    if len(list1) <= 0:
        log_error('error: no data: %s', sql)
        return my_fail_id

    row = list1[0]
    bus_id = row['bus_id']
    # log_debug('tgt - bus_id   -- %s', bus_id)

    return bus_id



def get_target_sub_bus_id(_src_relation_id, _tgt_relation_id, _src_cursor, _tgt_cursor):
    global my_meta_id
    global my_fail_id

    project_id, bus_id, sub_id = parse_relation_id(_src_relation_id)

    if my_is_meta_id(sub_id):
        # log_debug('sub-bus-id is meta: %s', sub_id)
        return sub_id

    sql = "select sub_bus_name from est_sub_bus where project_id = '%s' and  bus_id='%s' and sub_bus_id = '%s'" % (project_id, bus_id, sub_id)
    _src_cursor.execute(sql)
    list1 = _src_cursor.fetchall()

    if len(list1) <= 0:
        log_error('error: no data: %s', sql)
        return my_fail_id

    row = list1[0]
    sub_name = row['sub_bus_name']
    # log_debug('src - sub_bus_name -- %s', sub_name)


    project_id2, bus_id2, sub_id2 = parse_relation_id(_tgt_relation_id)
    sql = "select sub_bus_id from est_sub_bus where project_id = '%s' and bus_id='%s' and sub_bus_name = '%s'" % (project_id2, bus_id2, sub_name)
    _tgt_cursor.execute(sql)
    list1 = _tgt_cursor.fetchall()

    if len(list1) <= 0:
        log_error('error: no data: %s', sql)
        return my_fail_id

    row = list1[0]
    sub_bus_id = row['sub_bus_id']
    # log_debug('tgt - sub_bus_id   -- %s', sub_bus_id)

    return sub_bus_id



def translate_relation_id(_src_relation_id, _src_cursor, _tgt_cursor):
    global my_fail_id

    project_id, bus_id, sub_id = parse_relation_id(_src_relation_id)

    project_id2 = get_target_project_id(_src_relation_id, _src_cursor, _tgt_cursor)
    if project_id2 == my_fail_id:
        log_error('error: get_target_project_id')
        return ""

    new_relation_id = project_id2+bus_id+sub_id
    # log_debug('2: %s => %s', project_id2, new_relation_id)

    bus_id2 = get_target_bus_id(_src_relation_id, new_relation_id, _src_cursor, _tgt_cursor)
    if bus_id2 == my_fail_id:
        log_error('error: get_target_bus_id')
        return ""
    new_relation_id = project_id2+bus_id2+sub_id
    # log_debug('2: %s => %s', bus_id2, new_relation_id)

    sub_id2 = get_target_sub_bus_id(_src_relation_id, new_relation_id, _src_cursor, _tgt_cursor)
    if sub_id2 == my_fail_id:
        log_error('error: get_target_sub_bus_id')
        return ""
    new_relation_id = project_id2+bus_id2+sub_id2
    # log_debug('2: %s => %s', sub_id2, new_relation_id)

    return new_relation_id



def my_get_next_id(_id_type, _cursor):
    next_id = ""

    sql = "SELECT id_type, id_no FROM est_id_mng WITH( TABLOCKX ) WHERE id_type = '%s'" % (_id_type)
    # log_debug(sql)

    _cursor.execute(sql)
    list1 = _cursor.fetchall()

    sql = ""
    this_id = 0
    next_id = 0
    if len(list1) == 0:
        # log_info('first time')
        next_id = 1
        sql = "INSERT INTO est_id_mng WITH(TABLOCKX, UPDLOCK) (id_type , id_desc , id_no ) VALUES ( '%s' , '%s' , '%05d' ) " % ( _id_type , "AUTOM", next_id)
    else:
        # log_info('old guy')
        row = list1[0]
        this_id = int(row['id_no'])
        next_id = this_id + 1
        if next_id > 99999:
            elog("error: too big: %d", next_id)
            return "-1"

        sql = "UPDATE est_id_mng WITH(TABLOCKX, UPDLOCK) SET id_no= '%05d' WHERE id_type = '%s'" % (next_id, _id_type)

    # log_debug("sql -- [%s]", sql)
    _cursor.execute(sql)


    return '%05d' % next_id


# select flow_id from est_flow where flow_name = 'pub'
# input -- sub-table-name
def sub_get_res_id(_sub_table, _res_name, _cursor):

    table_name  = ''  # est_flow
    res_id_col  = ''  # flow_id
    res_name_col= ''  # flow_name

    my_key = '%s-GET_RES_ID' % (_sub_table)

    if not MyCtx.res_cvt_rule_map.has_key(my_key):
        log_error('error: cvt-rule not found: %s', my_key)
        return ''
    else:
        val_list    = MyCtx.res_cvt_rule_map[my_key]
        table_name  = val_list[0]
        res_id_col  = val_list[1]
        res_name_col= val_list[2]


    sql = "select %s from %s where %s = '%s'" % (res_id_col, table_name, res_name_col, _res_name)
    # log_debug('%s', sql)

    _cursor.execute(sql)
    list0 = _cursor.fetchall()
    if len(list0) <= 0:
        log_error('error: no data: %s', sql)
        return ""

    row = list0[0]
    res_id = row[res_id_col]
    log_debug('sub-update res_id [%s => %s]', res_id_col, res_id)

    return res_id



# dev.func_id => dev.func_name => sit.func_id
# step1. get RES-name from DEV
# step2. get RES-id   from SIT
def convert_res_id_one(_table, _column, _column_value, _src_cursor, _tgt_cursor):
    new_res_id = ''

    # select func_id from est_func where func_name = 'get_time'

    table_name  = ''  # est_func
    res_id_col  = ''  # func_id
    res_name_col= ''  # func_name
    res_name_val= ''  # as 'get_time'

    my_key = '%s-%s' % (_table, _column)


    if not MyCtx.res_cvt_rule_map.has_key(my_key):
        log_error('error: cvt-rule not found: %s', my_key)
        return ''
    else:
        val_list    = MyCtx.res_cvt_rule_map[my_key]
        table_name  = val_list[0]
        res_id_col  = val_list[1]
        res_name_col= val_list[2]

    res_id_val = _column_value
    sql = "select %s from %s where %s = '%s'" % (res_name_col, table_name, res_id_col, res_id_val)
    # log_debug('sql1: [%s]', sql)

    _src_cursor.execute(sql)
    list0 = _src_cursor.fetchall()
    if len(list0) <= 0:
        log_error('error: no data: %s', sql)
        return res_name

    row = list0[0]
    res_name = row[res_name_col].strip()
    # log_debug('res_name[%s => %s]', res_name_col, res_name)

    # log_debug('%s, %s, %s', res_id_col, res_name_col, res_name_val)

    sql = "select %s from %s where %s = '%s'" % (res_id_col, table_name, res_name_col, res_name)
    # log_debug('%s', sql)

    _tgt_cursor.execute(sql)
    list0 = _tgt_cursor.fetchall()
    if len(list0) <= 0:
        log_error('error: no data: %s', sql)
        return res_name

    row = list0[0]
    new_res_id = row[res_id_col]
    # log_debug('res_id [%s => %s]', res_id_col, new_res_id)


    return new_res_id



def convert_res_id(_table, _log_row, _src_cursor, _tgt_cursor):

    my_key = '%s' % (_table)

    if not MyCtx.res_cvt_list_map.has_key(my_key):
        # log_info('no need to change res-id for: %s', my_key)
        return ''
    else:
        val_list    = MyCtx.res_cvt_list_map[my_key]
        # log_debug('convert-list: %s => %s', my_key, val_list)


    for column in val_list:
        column_value = _log_row[column]
        # log_debug('column need to changed: [%s], [%s]', column, column_value)

        new_column_value = convert_res_id_one(_table, column, column_value, _src_cursor, _tgt_cursor)

        _log_row[column] = new_column_value

        # log_debug('column changed: [%s.%s], [%s => %s]', _table, column, column_value, new_column_value)

    return



def generate_res_id(_table, _log_row, _tgt_cursor):

    my_key = '%s' % (_table)

    if not MyCtx.res_gen_list_map.has_key(my_key):
        # log_info('no need to generate res-id for: %s', my_key)
        return ''
    else:
        val_list    = MyCtx.res_gen_list_map[my_key]
        # log_debug('generate-list: %s => %s', my_key, val_list)


    for column in val_list:
        # column                           # elem_id

        if not MyCtx.id_mng_map.has_key(column):
            continue

        id_type = MyCtx.id_mng_map[column] # '16'

        column_value = _log_row[column]    # 00079
        # log_debug('column need to generate: [%s], [%s]', column, column_value)

        new_res_id = my_get_next_id(id_type, _tgt_cursor)

        _log_row[column] = new_res_id

        # log_debug('ID generated: [%s.%s], [%s => %s]', _table, column, column_value, new_res_id)

    return



def convert_relation_id(_table, _log_row, _src_cursor, _tgt_cursor):

    my_key = '%s' % (_table)

    if not MyCtx.rel_conv_list_map.has_key(my_key):
        # log_info('no need to convert rel-id for: %s', my_key)
        return ''
    else:
        val_list    = MyCtx.rel_conv_list_map[my_key]
        # log_debug('convert-rel-list: %s => %s', my_key, val_list)


    for column in val_list:
        # column                            # relation_id

        relation_id = _log_row[column]      # 000040000100000
        # log_debug('column need to generate: [%s], [%s]', column, column_value)

        new_relation_id  = translate_relation_id(relation_id, _src_cursor, _tgt_cursor)
        if len(new_relation_id) == 0:
            log_error('error: translate_relation_id: %s', relation_id)
            return -1

        _log_row[column] = new_relation_id

        # log_debug('REL converted: [%s.%s], [%s => %s]', _table, column, relation_id, new_relation_id)


    return


#######################################################################



# main()
if __name__=="__main__":
    sailog_set("myfunc.log")


###########################################################################


#
