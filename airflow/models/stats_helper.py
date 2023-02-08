from airflow.settings import Stats


def stats_incr_helper(stat, value, dag_id, task_id):
    """ helper method to call Stats """
    Stats.incr(stat, value, 1, tags=['dag_id:{}'.format(dag_id)])

    task_id_entity = '.'.join(task_id.split('.')[:-1])
    task_id_action = '.'.join(task_id.split('.')[-1:])
    Stats.incr(stat + '.by_task', value, 1, tags=['dag_id:{}'.format(dag_id),
                                                  'task_id_prefix:{}'.format(task_id_entity),
                                                  'task_id_suffix:{}'.format(task_id_action)])


def stats_gauge_helper(stat, value, dag_id, task_id):
    """ helper method to call Stats """
    Stats.gauge(stat, value, 1, tags=['dag_id:{}'.format(dag_id)])

    task_id_entity = '.'.join(task_id.split('.')[:-1])
    task_id_action = '.'.join(task_id.split('.')[-1:])
    Stats.gauge(stat + '.by_task', value, 1, tags=['dag_id:{}'.format(dag_id),
                                                   'task_id_prefix:{}'.format(task_id_entity),
                                                   'task_id_suffix:{}'.format(task_id_action)])
