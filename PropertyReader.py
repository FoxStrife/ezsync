import configparser

class PropertyReader:

    def readProperty(self, strSection, strKey):
        config = configparser.RawConfigParser() 
        config.read('Config.properties')    
        strValue = config.get(strSection,strKey);   
        print ("Value captured for "+strKey+" :"+strValue)  
        return strValue