#!/bin/env python
# _*_ coding:utf8 _*_
import sys
import os
current_path = os.path.split(os.path.realpath(sys.argv[0]))[0]
path = os.path.dirname(current_path)
sys.path.append(path)
from core import binlog

def main():
    binlog.main()

if __name__ == "__main__":
    main()
