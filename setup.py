from setuptools import setup, find_packages

setup(
    name='apachebeamdependencies',
    version='0.1',
    description='Dependencies',
    install_requires = ['cloud-sql-python-connector[pymysql]','SQLAlchemy'],
    packages=find_packages()
)