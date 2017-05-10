class User(Module):
    __table__ = 'users'

    id =  IntegerField(primary_key = True)
    name = StringField()