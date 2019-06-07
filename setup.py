from setuptools import setup

setup(
    name='clusterun',
    version='0.0.2b4',
    description='A script to run jobs on a Torque server',
    url='https://github.com/justinnhli/clusterun',
    author='Justin Li',
    author_email='justinnhli@gmail.com',
    license='MIT',
    packages=['clusterun'],
    zip_safe=False,
    entry_points = {
        'console_scripts': [
            'clusterun=clusterun.clusterun:clusterun',
        ],
    }
)
