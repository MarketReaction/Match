package uk.co.jassoft.markets.match;

import uk.co.jassoft.markets.datamodel.company.CompanyBuilder;
import uk.co.jassoft.markets.datamodel.company.ExchangeBuilder;
import uk.co.jassoft.markets.datamodel.story.*;
import uk.co.jassoft.markets.repository.CompanyRepository;
import uk.co.jassoft.markets.repository.ExchangeRepository;
import uk.co.jassoft.markets.repository.StoryRepository;
import uk.co.jassoft.utils.BaseRepositoryTest;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.jms.TextMessage;
import java.net.URL;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Created by jonshaw on 18/03/2016.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = SpringConfiguration.class)
@IntegrationTest
public class StoryMatchListenerTest extends BaseRepositoryTest {

    @Autowired
    private StoryRepository storyRepository;

    @Autowired
    private ExchangeRepository exchangeRepository;

    @Autowired
    private CompanyRepository companyRepository;

    @Autowired
    private StoryMatchListener target;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        storyRepository.deleteAll();
        exchangeRepository.deleteAll();
    }

    @Test
    public void onMessage_storyWithNoEntities_isDeleted() throws Exception {

        String storyId = storyRepository.save(new StoryBuilder()
                .setUrl(new URL("http://test.com"))
                .createStory())
                .getId();

        TextMessage textMessage = new ActiveMQTextMessage();
        textMessage.setText(storyId);

        target.onMessage(textMessage);

        assertEquals(0, storyRepository.count());
    }

    @Test
    public void onMessage_storyWithNoMatchingCompanies_isDeleted() throws Exception {

        String storyId = storyRepository.save(new StoryBuilder()
                .setUrl(new URL("http://test.com"))
                .setEntities(NamedEntitiesBuilder.aNamedEntities()
                        .withOrganisation(NamedEntityBuilder.aNamedEntity()
                                .withName("TestOrganisation")
                                .withCount(2)
                                .withSentiments(Arrays.asList(SentimentBuilder.aSentiment()
                                        .withSentence("This is a really good sentiment for testing sentiment")
                                        .build()))
                                .build())
                        .build())
                .createStory())
                .getId();

        TextMessage textMessage = new ActiveMQTextMessage();
        textMessage.setText(storyId);

        target.onMessage(textMessage);

        assertEquals(0, storyRepository.count());
    }

    @Test
    public void onMessage_storyWithMatchingCompany_isMatched() throws Exception {

        String exchangeId = exchangeRepository.save(ExchangeBuilder.anExchange()
                .withEnabled(true)
                .build())
                .getId();

        String companyId = companyRepository.save(CompanyBuilder.aCompany()
                .withExchange(exchangeId)
                .withName("TestOrganisation")
                .withEntities(NamedEntitiesBuilder.aNamedEntities()
                        .withOrganisation(NamedEntityBuilder.aNamedEntity()
                                .withName("TestOrganisation")
                                .withCount(5)
                                .build())
                        .build())
                .build())
                .getId();

        String storyId = storyRepository.save(new StoryBuilder()
                .setUrl(new URL("http://test.com"))
                .setEntities(NamedEntitiesBuilder.aNamedEntities()
                        .withOrganisation(NamedEntityBuilder.aNamedEntity()
                                .withName("TestOrganisation")
                                .withCount(2)
                                .withSentiments(Arrays.asList(SentimentBuilder.aSentiment()
                                        .withSentence("This is a really good sentiment for testing sentiment")
                                        .build()))
                                .build())
                        .build())
                .createStory())
                .getId();

        TextMessage textMessage = new ActiveMQTextMessage();
        textMessage.setText(storyId);

        target.onMessage(textMessage);

        assertEquals(1, storyRepository.count());

        final Story story = storyRepository.findOne(storyId);

        assertEquals(1, story.getMatchedCompanies().size());
        assertEquals(companyId, story.getMatchedCompanies().get(0));
    }
}