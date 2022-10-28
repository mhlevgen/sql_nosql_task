import cassandra
import pandas as pd
import sqlalchemy.engine
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from sqlalchemy import create_engine


conn_string = 'postgresql://postgres:my_password@localhost:54320/postgres'

db = create_engine(conn_string)

ap = PlainTextAuthProvider(username='cassandra', password='cassandra')
clstr = Cluster(auth_provider=ap)
session = clstr.connect()
session.execute("""create keyspace mykeyspace with replication={
   'class': 'SimpleStrategy', 'replication_factor' : 1
};""")
session = clstr.connect('mykeyspace')


def insert_pg(*, path_to_table: str, engine: sqlalchemy.engine):
    conn = engine.connect()
    data = pd.read_csv(path_to_table, sep='\t')
    data.columns = [i.lower() for i in data.columns]
    data.to_sql('testset_b', con=conn, if_exists='replace', index=False)
    conn.close()


def select_pg(*, query: str, engine: sqlalchemy.engine, column_names: list[str]):
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()

    data = pd.DataFrame(result, columns=column_names)
    return data


def create_table_cassandra(*, query: str, session: cassandra.cluster.Session):
    session.execute(query)


def insert_cassandra(*, df: pd.DataFrame, cassandra_table: str, cassandra_colums: list[str]):
    stmt = session.prepare(f"""
    INSERT
    INTO
    {cassandra_table} ({', '.join(cassandra_colums)})
    VALUES({','.join(['?' for _ in cassandra_colums])})
    """)
    for row in df.values:
        session.execute(stmt, row)


if __name__ == "__main__":
    insert_pg(path_to_table='data/testset_B.tsv', engine=db)

    min_max_hdd_data = select_pg(
        query="""SELECT 'hdd', MIN(hdd_gb), MAX(hdd_gb) FROM testset_b""",
        engine=db,
        column_names=['type', 'min_gb', 'max_gb']
    )

    median_ghz_data = select_pg(
        query="""SELECT ram_gb, percentile_cont(0.5) WITHIN GROUP (ORDER BY ghz) AS median FROM testset_b GROUP BY ram_gb""",
        engine=db,
        column_names=['ram_gb', 'ghz_median']
    )

    rank_price_data = select_pg(
        query="""SELECT brand, price, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price) AS rank FROM testset_b""",
        engine=db,
        column_names=['brand', 'price', 'price_rank']
    )

    create_table_cassandra(
        query=
        '''CREATE TABLE median_ghz (
       ram_gb float,
       median_ghz float,
       PRIMARY KEY(ram_gb)
    );
    ''', session=session
    )

    create_table_cassandra(
        query=
        '''CREATE TABLE min_max_values (
       type TEXT,
       min_gb float,
       max_gb float,
       PRIMARY KEY(type)
    );
    ''', session=session
    )

    create_table_cassandra(
        query=
        '''CREATE TABLE rank_price (
       brand TEXT,
       price float,
       price_rank bigint,
       PRIMARY KEY(brand,  price_rank)
    );
    ''', session=session
    )

    insert_cassandra(
        df=min_max_hdd_data,
        cassandra_table='min_max_values',
        cassandra_colums=['type', 'min_gb', 'max_gb']
    )
    insert_cassandra(
        df=median_ghz_data,
        cassandra_table='median_ghz',
        cassandra_colums=['ram_gb', 'median_ghz']
    )
    insert_cassandra(
        df=rank_price_data,
        cassandra_table='rank_price',
        cassandra_colums=['brand', 'price', 'price_rank']
    )

    # check cassandra

    min_max_values_cass = session.execute("""select * from min_max_values""").all()
    assert min_max_values_cass[0].type == 'hdd'
    assert min_max_values_cass[0].max_gb == 2000
    assert min_max_values_cass[0].min_gb == 500

    median_ghz_cass = session.execute("""select * from median_ghz where ram_gb = 8""").all()
    assert median_ghz_cass[0].median_ghz == 2.5

    rank_price_cass = session.execute("""select * from rank_price where brand = 'Acer' and price_rank = 1""").all()
    assert rank_price_cass[0].price == 23900
