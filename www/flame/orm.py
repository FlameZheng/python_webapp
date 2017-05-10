import aiomysql
import logging;

import asyncio
from attr import attrs

logging.basicConfig(level=logging.INFO)


# 创建连接池
async def create_pool(loop,**kw):
    logging.info
    global __pool
    __pool = await aiomysql.create_pool(
        host = kw.get('host','localhost'),
        port = kw.get('port',3306),
        user = kw['user'],
        password = kw['password'],
        db = kw['db'],
        charser = kw.get('charset','utf8'),
        autocomit = kw.get('autocommit',True),
        maxsize= kw.get('maxsize',10),
        minsize = kw.get('maxsize',10),
        loop = loop
    )

async def select(sql,args,size = None):
    logging.log(sql, args)
    global __pool
    with (await __pool) as conn:
        cur = await conn.cursor(aiomysql.DictCursor)
        await cur.execute(sql.replace('?','%s'),args or ())
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        await cur.close()
        logging.info('rows returned : %s ' % len(rs))
        return rs

async def execute(sql,args):
    logging.log(sql,args)
    with (await __pool) as conn:
        try:
            cur = await conn.cursor(aiomysql.DictCursor)
            await cur.execute(sql.replace('?', '%s'),args )
            # 通过rowcount返回结果数
            affected = cur.rowcount
            await cur.close
        except BaseException as e:
            raise
        return affected

# 任何继承自Model的类（比如User）,会自动通过ModelMetaclass扫描映射关系，并存储到自身的类属性
# 如__table__、__mappings__中
class ModelMetaclass(type):
    # 排除model类 本身
    def __new__(cls, name, bases,attrs):
        if name == 'Model':
            return type.__new__(cls,name,bases,attrs)
        # 获取table名称
        tableName = attrs.get('__table__',None) or name
        logging.info('found model: %s (table：%s)'%(name,tableName))
        # 获取所有的Field和主键名：
        mappings = dict()
        fields = []
        primaryKey = None
        for k,v in attrs.items():
            if isinstance(v,Field):
                logging.info(' found mapping:%s ==> %s'% k,v)
                mappings[k] = v
                if v.primary_key:
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field :%s' %k)
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('Primary Key Not Found')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' % f,fields))
        attrs['__mappings__'] =mappings # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey # 主键属性名
        attrs['__fields__'] = fields # 除主键外的属性名
        # 构造默认的SELECT,INSERT,UPDATE和DELETE语句:
        attrs['__select__'] = 'select `%s`,%s from `%s`' %(primaryKey,','.join(escaped_fields),tableName)
        attrs['__insert__'] = 'insert into `%s` (%S,%s) values(%s)' % \
                              (tableName,','.join(escaped_fields),primaryKey,create_args_string(len(escaped_fields)+1))
        attrs['_update__'] = 'update `%s` set %s where `%s` = ?'% \
                             (tableName,','.join(map(lambda f:'`%s`=?'%(mappings.get(f).name or f),fields)),primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s` = ?' %(tableName,primaryKey)
        return type.__new__(cls,name,bases,attrs)

def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)

# 所有orm映射类的基类
class Model(dict,metaclass = ModelMetaclass):
    def __init__(self,**kw):
        super(Model,self).__init__(**kw)
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)
    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self,key):
        return getattr(self,key,None)

    def getValueOrDefault(self,key):
        value = getattr(self,key,None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s:%s ' % (key,str(value)))
                setattr(self,key,value)
        return value

    # 添加class方法，可以让所有的子类调用class方法：
    @classmethod
    @asyncio.coroutine
    def find(cls,pk):
        'find object by primary key'
        rs = yield from select('%s where `%s` = ?'% (cls.__select__,cls.__primary_key__),[pk],1)
        if len(rs)==0:
            return None
        return cls(**rs[0])

        # 往model中添加实例方法，就可以让所有子类调用实例方法：
    @asyncio.coroutine
    def save(self):
        args = list(map(self.getValueOrDefault,self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = yield from execute(self.__insert__,args)
        if rows != 1:
            logging.warn('failed to insert record:affected rows :%s '% rows)

    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warn('failed to remove by primary key: affected rows: %s' % rows)

    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        ' find objects by where clause. '
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        ' find number by select and where. '
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

# Field和各种Field的子类
class Field(object):
    def __init__(self,name,column_type,primary_key,default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default
    def __str__(self):
        return '<%s,%s:%s>' % (self.__class__.__name__,self.column_type,self.name)
# 映射varchar的StringField
class StringField(Field):
    def __init__(self,name = None,primary_key = False,default = None,ddl= 'varchar(100)'):
        super().__init__(name,ddl,primary_key,default)
# boolean --> Boolean
class BooleanField(Field):
    def __init__(self,name = None,default = False):
        super().__init__(name,'boolean',default)

# bigint --> Integer
class IntegerField(Field):
    def __init__(self,name = None,primary_key = False,default = 0):
        super().__init__(name,'bigint',primary_key,default)

# real --> Float
class FloatField(Field):
    def __init__(self,name = None,primary_key = False,default = 0.0):
        super().__init__(name,'real',primary_key,default)

class TextField(Field):
    def __init__(self,name=None,primary_key =False,default = None):
        super().__init__(name,'text',primary_key,default)
