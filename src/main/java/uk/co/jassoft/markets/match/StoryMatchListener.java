/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package uk.co.jassoft.markets.match;

import uk.co.jassoft.markets.datamodel.company.Company;
import uk.co.jassoft.markets.datamodel.story.NamedEntity;
import uk.co.jassoft.markets.datamodel.story.Story;
import uk.co.jassoft.markets.datamodel.story.metric.Metric;
import uk.co.jassoft.markets.datamodel.story.metric.MetricBuilder;
import uk.co.jassoft.markets.datamodel.system.Queue;
import uk.co.jassoft.markets.repository.ExchangeRepository;
import uk.co.jassoft.markets.repository.StoryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.util.*;
import java.util.stream.Collectors;

;

/**
 *
 * @author Jonny
 */
@Component
public class StoryMatchListener implements MessageListener {

    private static final Logger LOG = LoggerFactory.getLogger(StoryMatchListener.class);

    @Autowired
    private StoryRepository storyRepository;

    @Autowired
    private ExchangeRepository exchangeRepository;

    @Autowired
    protected MongoTemplate mongoTemplate;

    @Autowired
    private JmsTemplate jmsTemplate;
                
    void matchFound(final String message) {
        jmsTemplate.convertAndSend(Queue.MatchFound.toString(), message);
    }
    
    @Override
    @JmsListener(destination = "IndexedStory")
    public void onMessage(Message message)
    {
        final Date start = new Date();
        if ( message instanceof TextMessage )
        {
            final TextMessage textMessage = (TextMessage) message;
            try
            {
                Story story = storyRepository.findOne(textMessage.getText());
                
                if(story == null) {
                    LOG.warn("Story Does not exist any more Stopping processing at [{}]", this.getClass().getName());
                    return;
                }
                
                List<String> organisations = new ArrayList<>();

                if (story.getEntities() == null) {
                    LOG.warn("URL [{}] Has no Entities Stopping processing at [{}]", story.getUrl().toString(), this.getClass().getName());
                    storyRepository.delete(story.getId());
                    return;
                }

                List<String> enabledExchanges = exchangeRepository.findByEnabledIsTrue(new Sort(Sort.Direction.ASC, "name")).stream().map(exchange -> exchange.getId()).collect(Collectors.toList());

                organisations.addAll(story.getEntities().getOrganisations().stream().map(NamedEntity::getName).collect(Collectors.toList()));

                Query query1 = Query.query(Criteria.where("entities.organisations.name").in(organisations)
                        .orOperator(Criteria.where("name").in(organisations))
                        .andOperator(Criteria.where("exchange").in(enabledExchanges)));

                query1.fields().exclude("sentiments");

                List<Company> companies = mongoTemplate.find(query1, Company.class);

                for (Company company : companies)        
                {
                    boolean match = false;

                    // Loop organisations in story
                    for(String name : organisations)
                    {
                        if(company.getEntities() != null && company.getEntities().getOrganisations().contains(new NamedEntity(name)))
                        {
                            Optional<NamedEntity> companyEntity = getEntityByName(company.getEntities().getOrganisations(), name);

                            if(companyEntity.isPresent())
                            {
                                // story entity matches company entity
                                Optional<NamedEntity> storyEntity = getEntityByName(story.getEntities().getOrganisations(), name);

                                if(storyEntity.isPresent()) {
                                    match = true;
                                    storyEntity.get().setMatched(true);
                                    break;
                                }
                            }
                        }

                        if(company.getName().equalsIgnoreCase(name))
                        {
                            // story entity matches company nae
                            Optional<NamedEntity> storyEntity = getEntityByName(story.getEntities().getOrganisations(), name);

                            if (storyEntity.isPresent()) {
                                match = true;
                                storyEntity.get().setMatched(true);
                                break;
                            }
                        }
                    }

                    if(match) {
                        LOG.info("Match found for company [{}] id [{}]", company.getName(), company.getId());
                        story.getMatchedCompanies().add(company.getId());
                    }
                }

                if(!story.getMatchedCompanies().isEmpty()) {
                    storyRepository.save(story);

                    matchFound(story.getId());
                }
                else {
                    LOG.info("Removing Story [{}] as it doesnt match any companies", story.getId());
                    mongoTemplate.remove(story);
                }

                Metric metric = MetricBuilder.aMatchMetric().withStart(start).withEndNow().withDetail(String.format("Matched against %s Companies", story.getMatchedCompanies().size())).build();
                mongoTemplate.updateFirst(Query.query(Criteria.where("id").is(story.getId())), new Update().push("metrics", metric), Story.class);

            }
            catch (final Exception exception) {
                LOG.error(exception.getLocalizedMessage(), exception);
                
                throw new RuntimeException(exception);
            }
        }
    }
    
    private Optional<NamedEntity> getEntityByName(java.util.Collection<NamedEntity> entities, String name)
    {
        return entities.stream().filter(namedEntity -> namedEntity.equals(new NamedEntity(name))).findFirst();
    }
}
