package com.sma.hbm.scrollable;

import org.hibernate.Query;

public interface QueryInitializer {

    /**
     * Set all query parameters
     */
    void setQueryParameters(Query query);

}
