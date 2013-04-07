hbm-scrollable
==============

Traditionally a scrollable resultset would only transfer rows to the client on an as required basis. Unfortunately some jdbc driver like
the MySQL Connector/J fakes it, it executes the entire query and transports it to the client, so the driver actually has the entire result set loaded in RAM.

To avoid out of memory exception when playing with millions of rows (batches for example), one of the solutions is pagination.

This template (QueryTemplate) based on Hibernate is easy to use and do this workaround for you (pagination, scrolling and clearing Hibernate session).

