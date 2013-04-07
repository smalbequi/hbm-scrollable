package com.sma.hbm.scrollable;

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
 * To avoid out of memory exception when playing with millions of rows (batches for example), one of the solutions is pagination.
 * 
 * This template based on Hibernate is easy to use and do this workaround for you (pagination, scrolling and clearing Hibernate session).
 * 
 */

public class PaginatedQueryTemplate<T, E> extends QueryTemplate<T> {

    public PaginatedQueryTemplate(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    /**
     * Use this method with mysql (default fetch size is 1000)
     * 
     * Need more documentation ? See unit tests ;-)
     * 
     * @param hsql the hsql query
     * @param aliases all aliases used in the hsql query
     * @param initializer to set query parameters
     * @param function to apply on each fetch entity, must return the value of the ordered property
     */
    public void executeQuery(String hql, Iterable<String> aliases, PaginatedQueryInitializer<E> initializer, Function<T, E> function) {
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
     * @param function to apply on each fetch entity, must return the value of the ordered property
     * @param fetchSize for the underlying JDBC query (optimize perf or the memory usage)
     */
    public void executeQuery(String hql, Iterable<String> aliases, PaginatedQueryInitializer<E> initializer, Function<T, E> function,
            int fetchSize) {

        Session session = getSessionFactory().getCurrentSession();

        Query query = createQuery(hql, aliases, initializer, fetchSize, session).setMaxResults(fetchSize);

        boolean scroll = true;
        E lastOrderedValue = initializer.getOrderedQueryParameterFirstValue();

        while (scroll) {
            if (initializer != null) {
                initializer.setOrderedQueryParameter(query, lastOrderedValue);
            }
            ScrollableResults scrollableResults = query.scroll(ScrollMode.FORWARD_ONLY);
            try (QueryResults<T> queryResults = new QueryResults<T>(scrollableResults, session, fetchSize)) {
                int count = 0;
                T entity;
                while ((entity = queryResults.next()) != null) {
                    lastOrderedValue = function.apply(entity);
                    count++;
                }
                scroll = count == fetchSize;
            }
        }
    }

}
