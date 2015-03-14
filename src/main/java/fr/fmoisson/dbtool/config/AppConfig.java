package fr.fmoisson.dbtool.config;

import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.*;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;

@Configuration
@ComponentScan(basePackages = { "fr.fmoisson.dbtool"})
@EnableScheduling
@EnableAspectJAutoProxy
@EnableCaching
@EnableAsync
@EnableTransactionManagement
@PropertySource(value = {"file:db.properties"})
public class AppConfig
{
	private static Logger logger = LoggerFactory.getLogger(AppConfig.class);
	
	@Autowired
	private Environment env;
	
	@Autowired
	private DataSource dataSource;
	
	@Bean
	public static PropertySourcesPlaceholderConfigurer placeHolderConfigurer()
	{
		return new PropertySourcesPlaceholderConfigurer();
	}

	@Bean
	public JdbcTemplate jdbcTemplate() {
		return new JdbcTemplate(dataSource);
	}

	@Bean
	public DataSource dataSource()
	{
		BasicDataSource dataSource = new BasicDataSource();
		dataSource.setDriverClassName(env.getProperty("db.jdbc.driverClassName"));
		dataSource.setUrl(env.getProperty("db.jdbc.url"));
		dataSource.setUsername(env.getProperty("db.jdbc.username"));
		dataSource.setPassword(env.getProperty("db.jdbc.password"));
		return dataSource;
	}

	@Bean
	public CacheManager cacheManager()
	{
		return new ConcurrentMapCacheManager();
	}
	
}
