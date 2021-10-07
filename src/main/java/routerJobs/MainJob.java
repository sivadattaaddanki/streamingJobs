package routerJobs;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import allJobs.Job1;
import allJobs.Job2;


public class MainJob {

	private static final Logger logger = LoggerFactory.getLogger(MainJob.class);
	public static void main(String[] args) throws Exception {
	
	     if(args.length != 0) {
    		 ParameterTool parameters =  ParameterTool.fromArgs(args);	        
	         //String filePath = parameters.getRequired("file");
	         String jobType = parameters.getRequired("jobType");
	         
	         
	        if("job-1".equalsIgnoreCase(jobType)) {
	        	Job1.start();
	        }else if("job-2".equalsIgnoreCase(jobType)) {
	        	Job2.start();
	        }
	         else {
	        	 logger.error("Unable to start the job as arguments as unknown job type is passed");
	         }
    	}else {
    		logger.error("Unable to start the job as arguments are not passed");
    	}
	}
	
	
}
