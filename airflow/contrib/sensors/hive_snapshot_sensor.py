# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

from airflow.hooks.S3_hook import S3Hook
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class HiveSnapshotSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self,
                 dataset_source,
                 dataset_name,
                 partition_prefix='v=001/snapshot=',
                 airflow_env=os.environ['AIRFLOW_ENV'],
                 *args,
                 **kwargs):
        super(HiveSnapshotSensor, self).__init__(*args, **kwargs)
        self.dataset_name = dataset_name
        self.partition_prefix = partition_prefix
        self.airflow_env=airflow_env

    @staticmethod
    def _sort_list_of_dicts(list_of_dicts, key_name, reverse=False):
        decorated = [(d[key_name], d) for d in list_of_dicts]
        decorated.sort(reverse=reverse)
        return [d for (k, d) in decorated]

    def poke(self, context):
        next_execution_date = context['execution_date'] + context['dag'].schedule_interval
        next_snapshot_val = int(next_execution_date.strftime('%Y%m%d%H0000'))
        env = self.airflow_env
        bucket_name = '{env}-dwhlatest-text.datalake.nextdoor.com'.format(env=env)
        s3_hook = S3Hook()
        prefix = "{env}_nextdoor__{dataset_name}_hist/{partition_prefix}".format(env=env,
                                                                                 dataset_name=self.dataset_name,
                                                                                 partition_prefix=self.partition_prefix)

        prefixes = s3_hook.list_prefixes(bucket_name=bucket_name, prefix=prefix, delimiter='/')
        prefix_defs = []

        for prefix in prefixes:
            numeric_part = int(prefix.split('=')[2].strip('/'))
            prefix_val = int('{:<014d}'.format(numeric_part))
            prefix_defs.append({'val': prefix_val, 'name': prefix})

        prefix_defs_sorted = self._sort_list_of_dicts(prefix_defs, 'val', reverse=True)
        valid_prefixes = [p['name'] for p in prefix_defs_sorted if p['val'] >= next_snapshot_val]
        if valid_prefixes:
            min_prefix = valid_prefixes[-1].strip('/')
            snapshot = min_prefix.split('=')[2]
            ti = context['ti']
            ti.xcom_push('minimum_viable_snapshot', snapshot)
            return True
        return False
