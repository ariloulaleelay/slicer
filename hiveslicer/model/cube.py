import re
import logging
from sqlalchemy import Column, String, Text
from sqlalchemy import orm
from json import loads as json_loads
from json import dumps as json_dumps
from copy import deepcopy
from .base import Base
# from datetime import datetime

__all__ = [
    'Cube',
]

logger = logging.getLogger(__name__)


class Cube(Base):
    __tablename__ = 'cubes'

    name = Column(String, primary_key=True)
    config_string = Column(Text, nullable=False)
    hadoop_table = Column(String, nullable=False)
    hive_table_name = Column(String, nullable=False)

    def __init__(self, name, config, hadoop_table):
        self.name = name
        self.config = deepcopy(config)
        self.config_string = json_dumps(self.config)
        self.hadoop_table = hadoop_table
        self.hive_table_name = (name + '_' + hadoop_table).lower()
        self.hive_table_name = re.sub('[^a-z0-9]+', '_', self.hive_table_name)
        self.hive_table_name = re.sub('_+', '_', self.hive_table_name)
        self.hive_table_name = re.sub('^_|_$', '', self.hive_table_name)

    @orm.reconstructor
    def __init_reconstruct(self):
        self.config = json_loads(self.config_string)

    def create_hadoop_table(self, engine):
        columns = []
        for dimension in self.config['dimensions']:
            dim_type = 'STRING'
            if dimension.get('role', None) == 'time':
                dim_type = 'TIMESTAMP'
            columns.append({
                'name': dimension['name'],
                'type': dim_type
            })

        for measure in self.config['measures']:
            columns.append({
                'name': measure['name'],
                'type': 'FLOAT',
            })
        columns_string = ", ".join("`%s` %s" % (col['name'], col['type']) for col in columns)
        engine.execute("DROP TABLE IF EXISTS %s" % (self.hive_table_name))
        sql = """
            CREATE EXTERNAL TABLE %s
            (%s)
            ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
            LOCATION '%s'
        """ % (
            self.hive_table_name,
            columns_string,
            self.hadoop_table
        )
        engine.execute(sql)

    def aggregate(self, engine, aggregate, drilldown, cut):
        return None
