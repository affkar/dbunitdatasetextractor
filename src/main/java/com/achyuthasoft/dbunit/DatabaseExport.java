package com.achyuthasoft.dbunit;

import org.dbunit.database.AmbiguousTableNameException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.QueryDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.FileOutputStream;
import java.util.Arrays;

public class DatabaseExport {

    private static final Logger LOG = LoggerFactory.getLogger( DatabaseExport.class );
    
    private DataSource dataSource;

    public DatabaseExport(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void run(String... args) throws Exception {
        QueryDataSet partialDataSet = new QueryDataSet(new DatabaseConnection(dataSource.getConnection()));
        Arrays.asList(args).forEach(it-> {
            try {
                partialDataSet.addTable(it);
            } catch (AmbiguousTableNameException e) {
                LOG.warn("Error Extracting Table {}" , it, e);
            }
        });
        FlatXmlDataSet.write(partialDataSet, new FileOutputStream("partial.xml"));
    }
}
