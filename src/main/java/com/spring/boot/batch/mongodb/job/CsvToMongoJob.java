package com.spring.boot.batch.mongodb.job;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.spring.boot.batch.mongodb.model.AppUser;

@EnableBatchProcessing
@Configuration
public class CsvToMongoJob {

  @Autowired
  private JobBuilderFactory jobBuilderFactory;
  @Autowired
  private StepBuilderFactory stepBuilderFactory;

  @Autowired
  private MongoTemplate mongoTemplate;

  @Bean
  public Job readCSVFile() {
    return jobBuilderFactory.get("readUsersCSVDataFile").incrementer(new RunIdIncrementer()).start(step1())
        .build();
  }

  @Bean
  public Step step1() {
    return stepBuilderFactory.get("step1").<AppUser, AppUser>chunk(10).reader(reader())
        .writer(writer()).build();
  }

  @Bean
  public FlatFileItemReader<AppUser> reader() {
    FlatFileItemReader<AppUser> reader = new FlatFileItemReader<>();
    reader.setResource(new ClassPathResource("users.csv"));
    reader.setLineMapper(new DefaultLineMapper<AppUser>() {{
      setLineTokenizer(new DelimitedLineTokenizer() {{
        setNames(new String[]{"id", "username","password","useremail","userfirstname","userlastname","useraddress"});

      }});
      setFieldSetMapper(new BeanWrapperFieldSetMapper<AppUser>() {{
        setTargetType(AppUser.class);
      }});
    }});
    return reader;
  }

  @Bean
  public MongoItemWriter<AppUser> writer() {
    MongoItemWriter<AppUser> writer = new MongoItemWriter<AppUser>();
    writer.setTemplate(mongoTemplate);
    writer.setCollection("appusers");
    return writer;
  }
}
