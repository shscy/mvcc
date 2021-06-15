# mvcc

[![CI](https://github.com/wangrunji0408/mvcc/workflows/CI/badge.svg?branch=master)](https://github.com/wangrunji0408/mvcc/actions)


## 支持rollback 
## 支持多线程并发事务
+ 去除Begin 和Put函数的顺序绑定关系， 目前的实现中， 先执行的事务，必须先执行put，否则abort
## 支持CAS操作 