package com.sma.hbm.scrollable;

import org.hibernate.Query;

public interface QueryInitializer {

    void setQueryParameters(Query query);
}
