from typing import List, Optional

from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, ForeignKey, insert, select, bindparam
from sqlalchemy.orm import Session, DeclarativeBase, Mapped, mapped_column, relationship

engine = create_engine("sqlite+pysqlite:///:memory:", echo=True)
metadata_obj = MetaData()
user_table = Table(
    "user_account",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("name", String(30)),
    Column("fullname", String),
)

address_table = Table(
    "address",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("user_id", ForeignKey("user_account.id"), nullable=False),
    Column("email_address", String, nullable=False),
)

with engine.connect() as conn:
    result = conn.execute(text("CREATE TABLE some_table (x int, y int)"))
    conn.execute(
        text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
        [{"x": 1, "y": 1}, {"x": 2, "y": 4}],
    )
    conn.commit()
    
print("#######################################################################")
with engine.begin() as conn:
    conn.execute(
        text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
        [{"x": 6, "y": 8}, {"x": 9, "y": 10}],
    )
    
print("#######################################################################")
with engine.connect() as conn:
    result = conn.execute(text("SELECT x, y FROM some_table"))
    for row in result:
        print(f"x: {row.x} y: {row.y}")
        
print("#######################################################################")
with engine.connect() as conn:
    result = conn.execute(text("SELECT x, y FROM some_table WHERE y > :y"), {"y": 2})
    for row in result:
        print(f"x: {row.x} y: {row.y}")
        
print("#######################################################################")
with engine.connect() as conn:
    conn.execute(
        text("INSERT INTO some_table (x, y) VALUES (:x, :y)"),
        [{"x": 11, "y": 12}, {"x": 13, "y": 14}],
    )
    conn.commit()
    
print("#######################################################################")
stmt = text("SELECT x, y FROM some_table WHERE y > :y ORDER BY x, y")
with Session(engine) as session:
    result = session.execute(stmt, {"y": 6})
    for row in result:
        print(f"x: {row.x} y: {row.y}")
        
print("#######################################################################")
with Session(engine) as session:
    result = session.execute(
        text("UPDATE some_table SET y=:y WHERE x=:x"),
        [{"x": 9, "y": 11}, {"x": 13, "y": 15}],
    )
    session.commit()
    
print("######################################## Emitindo DDL para o banco de dados ########################################")
metadata_obj.create_all(engine)

print("######################################## Estabelecendo uma Base Declarativa ########################################")
class Base(DeclarativeBase):
    pass
print(Base.metadata)
print(Base.registry)

class User(Base):
    __tablename__ = "user_account"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(30))
    fullname: Mapped[Optional[str]]
    
    addresses: Mapped[List["Address"]] = relationship(back_populates="user")
    
    def __repr__(self) -> str:
        return f"User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})"
    
class Address(Base):
    __tablename__ = "address"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    email_address: Mapped[str]
    user_id = mapped_column(ForeignKey("user_account.id"))
    
    user: Mapped[User] = relationship(back_populates="addresses")
    
    def __repr__(self) -> str:
        return f"Address(id={self.id!r}, email_address={self.email_address!r})"
    
print("######################### Emitindo DDL para o banco de dados a partir de um mapeamento ORM #########################")
Base.metadata.create_all(engine)

print("###################################### A construção da expressão SQL insert() ######################################")
stmt = insert(user_table).values(name="spongebob", fullname="Spongebob Squarepants")
print(stmt)
compiled = stmt.compile()
print(compiled.params)

print("############################################## Executando a Declaração #############################################")
with engine.connect() as conn:
    result = conn.execute(stmt)
    conn.commit()
    print(result.inserted_primary_key)    
    
print("############################# INSERT geralmente gera a cláusula “values” automaticamente ###########################")
print(insert(user_table))
with engine.connect() as conn:
    result = conn.execute(
        insert(user_table),
        [
            {"name": "sandy", "fullname": "Sandy Cheeks"},
            {"name": "patrick", "fullname": "Patrick Start"},
        ],
    )
    conn.commit()
    
scalar_subq = (
    select(user_table.c.id)
    .where(user_table.c.name == bindparam("username"))
    .scalar_subquery()
)

with engine.connect() as conn:
    result = conn.execute(
        insert(address_table).values(user_id=scalar_subq),
        [
            {
                "username": "spongebob",
                "email_address": "spongbob@sqlalchemy.org",
            },
            {"username": "sandy", "email_address": "sandy@sqlalchemy.org"},
            {"username": "sandy", "email_address": "sandy@squirrelpower.org"},
        ],
    )
    conn.commit()
    
print("################################################# INSERIR…RETORNANDO ###############################################")
insert_stmt = insert(address_table).returning(
    address_table.c.id, address_table.c.email_address
)
print(insert_stmt)

select_stmt = select(user_table.c.id, user_table.c.name + "@aol.com")
insert_stmt = insert(address_table).from_select(
    ["user_id", "email_address"], select_stmt
)
print(insert_stmt.returning(address_table.c.id, address_table.c.email_address))

print("################################################# INSERIR…DE SELECIONAR ############################################")
select_stmt = select(user_table.c.id, user_table.c.name + "@aol.com")
insert_stmt = insert(address_table).from_select(
    ["user_id", "email_address"], select_stmt
)
print(insert_stmt)

print("######################################## A construção da expressão SQL select() ####################################")
stmt = select(user_table).where(user_table.c.name == "spongebob")
print(stmt)
with engine.connect() as conn:
    for row in conn.execute(stmt):
        print(row)
        
stmt = select(User).where(User.name == "spongebob")
with Session(engine) as session:
    for row in session.execute(stmt):
        print(row)
        
print("######################################### Definindo as cláusulas COLUMNS e FROM ####################################")
print(select(user_table))
print(select(user_table.c.name, user_table.c.fullname))
print(select(user_table.c["name", "fullname"]))

print("######################################### Selecionando Entidades e Colunas ORM #####################################")
print(select(User))
row = session.execute(select(User)).first()
print(row)
user = session.scalars(select(User)).first()
print(user)
print(select(User.name, User.fullname))
row = session.execute(select(User.name, User.fullname)).first()
print(row)
print(session.execute(
        select(User.name, Address).where(User.id == Address.user_id).order_by(Address.id)
    ).all()
)