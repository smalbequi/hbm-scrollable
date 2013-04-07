package com.sma.hbm.scrollable;

import static com.google.common.collect.Lists.*;
import static org.fest.assertions.Assertions.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.hibernate.Query;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.google.common.base.Function;
import com.google.common.base.Throwables;

public class QueryTemplateIntegrationTest {

    private SessionFactory sessionFactory;

    private QueryTemplate<Person> queryTemplate;

    private PaginatedQueryTemplate<Person, String> paginatedQueryTemplate;

    private Transaction transaction;

    @Before
    public void before() {
        Configuration configuration = new Configuration();
        configuration.setProperty(Environment.DRIVER, "org.hsqldb.jdbcDriver");
        configuration.setProperty(Environment.URL, "jdbc:hsqldb:mem:QueryTemplateIntegrationTest");
        configuration.setProperty(Environment.USER, "sa");
        configuration.setProperty(Environment.DIALECT, "org.hibernate.dialect.H2Dialect");
        configuration.setProperty(Environment.SHOW_SQL, "true");
        configuration.setProperty(Environment.HBM2DDL_AUTO, "create-drop");
        configuration.setProperty(Environment.CURRENT_SESSION_CONTEXT_CLASS, "thread");

        configuration.addAnnotatedClass(Person.class);

        sessionFactory = configuration.buildSessionFactory();

        queryTemplate = new QueryTemplate<Person>(sessionFactory);
        paginatedQueryTemplate = new PaginatedQueryTemplate<Person, String>(sessionFactory);

        sessionFactory.openSession();
        transaction = sessionFactory.getCurrentSession().beginTransaction();
        // Insert persons in database
        for (long i = 0; i <= 15; i++) {
            sessionFactory.getCurrentSession().save(new Person("name" + i));
        }
    }

    @Test
    public void executeQuery_based_only_on_db_fetching_feature() throws Exception {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            queryTemplate.executeQuery("from Person p where p.name like :name order by name asc", newArrayList("p"), //
                    new QueryInitializer() {

                        @Override
                        public void setQueryParameters(Query query) {
                            query.setString("name", "name1%");
                        }
                    }, new Function<Person, Void>() {

                        @Override
                        public Void apply(Person person) {
                            // Do something with person
                            try {
                                System.out.println(person.getName());
                                outputStream.write(person.getName().getBytes());
                                return null;
                            } catch (IOException e) {
                                throw Throwables.propagate(e);
                            }
                        }
                    }, 3);
            assertThat(new String(outputStream.toByteArray())).isEqualTo("name1name10name11name12name13name14name15");
        }
    }

    @Test
    public void executeQuery_based_on_pagination() throws Exception {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {

            paginatedQueryTemplate.executeQuery("from Person p where p.name like :name and p.name > :lastValue order by name asc",
                    newArrayList("p"),//
                    new PaginatedQueryInitializer<String>() {

                        @Override
                        public void setQueryParameters(Query query) {
                            query.setString("name", "name1%");
                        }

                        @Override
                        public String getOrderedQueryParameterFirstValue() {
                            return "";
                        }

                        @Override
                        public void setOrderedQueryParameter(Query query, String value) {
                            query.setString("lastValue", value);
                        }
                    },//
                    new Function<Person, String>() {

                        @Override
                        public String apply(Person person) {
                            // Do something with person and return ordered value
                            try {
                                System.out.println(person.getName());
                                outputStream.write(person.getName().getBytes());
                                return person.getName();
                            } catch (IOException e) {
                                throw Throwables.propagate(e);
                            }
                        }
                    }, 3);
            assertThat(new String(outputStream.toByteArray())).isEqualTo("name1name10name11name12name13name14name15");
        }
    }

    @After
    public void after() {
        transaction.rollback();
        sessionFactory.getCurrentSession().close();
        sessionFactory.close();
    }
}
