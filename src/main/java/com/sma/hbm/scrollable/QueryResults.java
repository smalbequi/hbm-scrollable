package com.sma.hbm.scrollable;

import org.hibernate.ScrollableResults;
import org.hibernate.Session;

public class QueryResults<T> implements AutoCloseable {

    private final ScrollableResults scrollableResults;

    private final Session session;

    private int count = 0;

    private final int fetchSize;

    QueryResults(ScrollableResults scrollableResults, Session session, int fetchSize) {
        this.scrollableResults = scrollableResults;
        this.session = session;
        this.fetchSize = fetchSize;
    }

    /**
     * Return the next entity or null if no more entity, and clear the session when fetch size threshold reached
     * 
     * @return the next entity or null if no more entity
     */
    @SuppressWarnings("unchecked")
    public T next() {
        T entity = scrollableResults.next() ? (T) scrollableResults.get(0) : null;

        if (entity != null && (++count % fetchSize == 0)) {
            session.clear();
        }

        return entity;
    }

    @Override
    public void close() {
        session.clear();
        scrollableResults.close();
    }

}
