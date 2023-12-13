# Prelude is intended to set up all relevant variables and supporting fixtures for execution of project scripts
# -------------------------------------------------------------------------------------------------------------
import sys,os
from pathlib import Path
from dataclasses import dataclass


__all__ = [
    "wd",
    "db",
]



# register project folders
@dataclass
class WorkingDirectories():
    """this class defines folders structure of the project that need to accessed
    """
    prj_dir:Path =          Path.cwd()
    code:Path =             prj_dir.joinpath("02.code")
    code_scripts:Path =     prj_dir.joinpath("02.code","scripts")
    code_sqls:Path =        prj_dir.joinpath("02.code","sqls")
    inputs:Path =           prj_dir.joinpath("03.inputs")
    inputs_static:Path =    prj_dir.joinpath("03.inputs","static")
    inputs_dynamic:Path =   prj_dir.joinpath("03.inputs","dynamic")
    outputs:Path =          prj_dir.joinpath("04.outputs")

wd = WorkingDirectories()

# register target files
db = wd.outputs.joinpath("database.db")

# define and register special sql function
import sqlite3



