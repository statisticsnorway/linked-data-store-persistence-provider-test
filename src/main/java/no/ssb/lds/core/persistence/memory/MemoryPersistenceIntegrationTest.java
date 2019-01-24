package no.ssb.lds.core.persistence.memory;

import no.ssb.lds.core.persistence.test.PersistenceIntegrationTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.util.Map;
import java.util.Set;

public class MemoryPersistenceIntegrationTest extends PersistenceIntegrationTest {

    public MemoryPersistenceIntegrationTest() {
        super("lds-provider-memory-integration-test");
    }

    @BeforeClass
    public void setup() {
        persistence = new MemoryInitializer().initialize(namespace,
                Map.of("persistence.fragment.capacity", "1024",
                        "persistence.mem.transaction.cancel-on-close", "true"),
                Set.of("Person", "Address", "FunkyLongAddress"));
    }

    @AfterClass
    public void teardown() {
        if (persistence != null) {
            persistence.close();
        }
    }
}
