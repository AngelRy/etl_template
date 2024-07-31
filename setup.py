from setuptools import find_packages,setup
from typing import List

HYP_E_DOT='-e .'

def get_requirements(file_path:str)->List[str]:
    '''
    this returns the list of requirements    
    '''
    with open(file_path) as file_obj:
        requirements=file_obj.readlines()
        requirements=[req.replace("\n","") for req in requirements]

        if HYP_E_DOT in requirements:
            requirements.remove(HYP_E_DOT)
        
    return requirements
setup(
name='etl_begg',
version='0.0.1',
author='Angelry',
author_email='angel.dzekov@gmail.com',
packages=find_packages(),
install_requires=get_requirements('requirements.txt')

    )