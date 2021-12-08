package utils;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;


public class Log {

	private static Map<String, Logger> logMap = new HashMap<String, Logger>();

	private Log() {
	}

	public static Logger getInstance(String className) {

		if (!logMap.containsKey(className)) {
			logMap.put(className, Logger.getLogger(className));
		}

		return logMap.get(className);
	}

	public static void err(String className, String message) {
		Logger log = getInstance(className);
		log.info("Error: " + message);
	}
}
