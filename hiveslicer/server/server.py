from cherrypy import expose
import cherrypy
from hiveslicer.model import Cube, Base
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy import create_engine

__all__ = [
    'SlicerServer',
]

Session = sessionmaker()

json_out = cherrypy.tools.json_out()
json_in = cherrypy.tools.json_in()


@cherrypy.popargs('cube_name')
class CubeView(object):

    def __init__(self, config, sql_engine, hadoop_engine):
        self._config = config
        self._sql_engine = sql_engine
        self._hadoop_engine = hadoop_engine

    def _open_session(self):
        return Session(bind=self._sql_engine)

    def _get_cube(self, cube_name):
        try:
            session = self._open_session()
            cube = session.query(Cube).filter(Cube.name == cube_name).first()
            return cube
        finally:
            session.close()
        return None

    @expose
    @json_out
    def index(self, cube_name):
        cube = self._get_cube(cube_name)
        if cube is None:
            return None
        return cube.config

    @expose
    @json_out
    def model(self, cube_name):
        return self.index(cube_name)

    @expose
    @json_out
    def aggregate(self, cube_name, aggregates=None, drilldown=None, cut=None):
        cube = self._get_cube(cube_name)
        if cube is None:
            return None
        return cube.aggregate(self._sql_engine, aggregates, drilldown, cut)


class SlicerServer(object):

    _version = "0.1"

    def __init__(self, config):
        self._config = config
        self._sql_engine = create_engine(self._config.hiveslicer.database.url())
        self._hadoop_engine = create_engine(self._config.hiveslicer.hive.sqlalchemy_url())
        for sql in self._config.hiveslicer.hive.execute_on_init():
            self._hadoop_engine.execute(sql)
        Base.metadata.create_all(self._sql_engine)
        self.cube = CubeView(config, self._sql_engine, self._hadoop_engine)

    def _open_session(self):
        return Session(bind=self._sql_engine)

    @expose
    def index(self):
        return "hiveslicer"

    @expose
    @json_out
    def version(self):
        return {
            "version": self._version,
        }

    @expose
    @json_out
    def info(self):
        return {
        }

    @expose
    @json_out
    def cubes(self):
        result = []
        session = self._open_session()
        cubes = session.query(Cube)
        for cube in cubes:
            result.append(cube.config)
        session.close()
        return result

    @expose
    @json_in
    @json_out
    def upload(self):
        try:
            session = self._open_session()
            data = cherrypy.request.json
            name = data['name']
            hadoop_table = data['hadoop_table']
            config = data

            cube = Cube(name, config, hadoop_table)
            cube.create_hadoop_table(self._hadoop_engine)
            session.merge(cube)
            session.commit()
        except Exception, e:
            return {"error": unicode(e)}
        finally:
            session.close()
        return {"success": "ok"}
