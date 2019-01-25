package no.ssb.lds.core.persistence.postgres;

import no.ssb.lds.api.persistence.json.JsonPersistence;
import no.ssb.lds.api.persistence.reactivex.RxJsonPersistenceBridge;
import no.ssb.lds.api.persistence.reactivex.RxPersistenceBridge;
import no.ssb.lds.api.persistence.streaming.Persistence;
import no.ssb.lds.core.persistence.test.PersistenceIntegrationTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.Map;
import java.util.Set;

public class PostgresIntegrationTest extends PersistenceIntegrationTest {

    public PostgresIntegrationTest() {
        super("lds-provider-postgres-integration-test");
    }

    @BeforeClass
    public void setup() {
        JsonPersistence jsonPersistence = new PostgresDbInitializer().initialize(namespace,
                Map.of("postgres.driver.host", "localhost",
                        "postgres.driver.port", "5432",
                        "postgres.driver.user", "lds",
                        "postgres.driver.password", "lds",
                        "postgres.driver.database", "lds",
                        "persistence.fragment.capacity", String.valueOf(Integer.MAX_VALUE)
                ),
                Set.of("Person", "Address", "FunkyLongAddress"));
        Persistence persistence = jsonPersistence.getPersistence();
        this.persistence = new RxJsonPersistenceBridge(
                new RxPersistenceBridge(persistence),
                Integer.MAX_VALUE
        );
    }

    @AfterClass
    public void teardown() {
        if (persistence != null) {
            persistence.close();
        }
    }
}
