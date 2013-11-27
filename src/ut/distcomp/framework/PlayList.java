package ut.distcomp.framework;

import java.util.Hashtable;

public class PlayList {
	Hashtable<String, String> playlist = new Hashtable<String, String>();
	
	void add(String songName, String URL){
		if(!playlist.containsKey(songName))
			playlist.put(songName,URL);
	}

	void delete(String songName){
		if(playlist.containsKey(songName))
			playlist.remove(songName);
	}

	void editName(String songName, String newsongName){
		if(playlist.containsKey(songName)){
			playlist.put(newsongName, playlist.get(songName));
			playlist.remove(songName);
		}	
	}

	void editUrl(String songName, String newURL){
		if(playlist.containsKey(songName))
			playlist.put(songName, newURL);
	}

	public void execute(String updateStr){
		String[] s=updateStr.split("[\\[;\\]]");
		switch(s[0]){
		case "add":
			add(s[1],s[2]);
		case "delete":
			delete(s[1]);
		case "edit":
			editUrl(s[1],s[2]);
		default:
			System.err.println("Unexpected update command: "+s[0]);
		}
	}
}
