package com.sma.hbm.scrollable;

import org.hibernate.CacheMode;
import org.hibernate.LockMode;
import org.hibernate.Query;
import org.hibernate.ScrollMode;
import org.hibernate.ScrollableResults;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import com.google.common.base.Function;

/**
 * 
 * Traditionally a scrollable resultset would only transfer rows to the client on an as required basis. Unfortunately some jdbc driver like
 * the MySQL Connector/J fakes it, it executes the entire query and transports it to the client, so the driver actually has the entire
 * result set loaded in RAM.
 * 
 * To avoid out of memory exception when playing with millions of rows (batches for example), one of the solutions is pagination
 * 
 * This template base on Hibernate is easy to used and do this workaround for you (pagination, scrolling and clearing Hibernate session)
 * 
 */

public class QueryTemplate<T> {

    private static final int DEFAULT_FETCH_SIZE = 1000;

    private final SessionFactory sessionFactory;

    public QueryTemplate(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    /**
     * Don't use this method with mysql
     * 
     * Need more documentation ? See unit tests ;-)
     * 
     * @param hsql the hsql query
     * @param aliases all aliases used in the hsql query
     * @param initializer to set query parameters
     * @return the scrollable query results
     * 
     */
    public QueryResults<T> executeQuery(String hql, Iterable<String> aliases, QueryInitializer initializer) {
        return executeQuery(hql, aliases, initializer, DEFAULT_FETCH_SIZE);
    }

    /**
     * Don't use this method with mysql
     * 
     * Need more documentation ? See unit tests ;-)
     * 
     * @param hsql the hsql query
     * @param aliases all aliases used in the hsql query
     * @param initializer to set query parameters
     * @param fetchSize for the underlying JDBC query (optimize perf or memory usage). Session is cleared when fetch size is reached.
     * @return the scrollable query results
     * 
     */
    public QueryResults<T> executeQuery(String hql, Iterable<String> aliases, QueryInitializer initializer, int fetchSize) {
        Session session = sessionFactory.getCurrentSession();

        Query query = createQuery(hql, aliases, initializer, fetchSize, session);

        ScrollableResults scrollableResults = query.scroll(ScrollMode.FORWARD_ONLY);

        return new QueryResults<T>(scrollableResults, session, fetchSize);
    }

    /**
     * Use this method with mysql (default fetch size is 1000)
     * 
     * Need more documentation ? See unit tests ;-)
     * 
     * @param hsql the hsql query
     * @param aliases all aliases used in the hsql query
     * @param initializer to set query parameters
     */
    public void executeQuery(String hql, Iterable<String> aliases, QueryInitializer initializer, Function<T, Void> function) {
        executeQuery(hql, aliases, initializer, function, DEFAULT_FETCH_SIZE);
    }

    /**
     * Use this method with mysql
     * 
     * Need more documentation ? See unit tests ;-)
     * 
     * @param hsql the hsql query
     * @param aliases all aliases used in the hsql query
     * @param initializer to set query parameters
     * @param fetchSize for the underlying JDBC query (optimize perf or the memory usage)
     */
    public void executeQuery(String hql, Iterable<String> aliases, QueryInitializer initializer, Function<T, Void> function, int fetchSize) {

        Session session = sessionFactory.getCurrentSession();

        Query query = createQuery(hql, aliases, initializer, fetchSize, session).setMaxResults(fetchSize);

        boolean scroll = true;
        int firstResult = 0;

        while (scroll) {
            query.setFirstResult(firstResult);
            ScrollableResults scrollableResults = query.scroll(ScrollMode.FORWARD_ONLY);
            try (QueryResults<T> queryResults = new QueryResults<T>(scrollableResults, session, fetchSize)) {
                int count = 0;
                T entity;
                while ((entity = queryResults.next()) != null) {
                    function.apply(entity);
                    count++;
                }
                scroll = count == fetchSize;
            }
            firstResult += fetchSize;
        }
    }

    private Query createQuery(String hql, Iterable<String> aliases, QueryInitializer initializer, int fetchSize, Session session) {
        Query query = session.createQuery(hql);

        query.setFetchSize(fetchSize);
        query.setReadOnly(true);
        query.setCacheable(false);
        query.setCacheMode(CacheMode.IGNORE);

        for (String alias : aliases) {
            query.setLockMode(alias, LockMode.NONE);
        }

        if (initializer != null) {
            initializer.setQueryParameters(query);
        }

        return query;
    }
}
