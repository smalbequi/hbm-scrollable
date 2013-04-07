package com.sma.hbm.scrollable;

import org.hibernate.Query;

public interface PaginatedQueryInitializer<T> extends QueryInitializer {

    /**
     * Return the first value of the query parameter used to paginate (often -1 or empty string)
     */
    T getOrderedQueryParameterFirstValue();

    /**
     * Set the value of the query parameter used to paginate with the last fetch value
     */
    void setOrderedQueryParameter(Query query, T value);
}
