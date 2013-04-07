package com.sma.hbm.scrollable;

import static org.fest.assertions.Assertions.*;
import static org.mockito.Mockito.*;
import org.hibernate.ScrollableResults;
import org.hibernate.Session;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.sma.hbm.scrollable.QueryResults;

@RunWith(MockitoJUnitRunner.class)
public class QueryResultsTest {

    @Mock
    private Session session;

    @Mock
    private ScrollableResults scrollableResults;

    @Test
    public void next_with_no_more_entity_must_return_null_clear_session() {
        when(scrollableResults.next()).thenReturn(false);
        try (QueryResults<Object> queryResults = new QueryResults<>(scrollableResults, session, 10)) {
            assertThat(queryResults.next()).isNull();
        }
        verify(session).clear();
        verify(scrollableResults).close();
    }

    @Test
    public void next_with_5_results_and_2_as_fetch_size_must_return_5_entities_and_clear_session_3_times() {
        when(scrollableResults.next()).thenReturn(true, true, true, true, true, false);
        when(scrollableResults.get(0)).thenReturn(new Object(), new Object(), new Object(), new Object(), new Object(), null);

        try (QueryResults<Object> queryResults = new QueryResults<>(scrollableResults, session, 2)) {
            assertThat(queryResults.next()).isNotNull();
            assertThat(queryResults.next()).isNotNull();
            assertThat(queryResults.next()).isNotNull();
            assertThat(queryResults.next()).isNotNull();
            assertThat(queryResults.next()).isNotNull();
            assertThat(queryResults.next()).isNull();
        }
        verify(session, times(3)).clear();
        verify(scrollableResults).close();
    }
}
