package com.alicja.test

import com.alicja.test.Utilities._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

/** Listens to a stream of Tweets and keeps track of the most popular
 *  hashtags over a 5 minute window.
 */
object PopularWords {
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    
    // Set up a Spark streaming context named "PopularHashtags" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "PopularWords", Seconds(1))
    
    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)
    
    // Now extract the text of each status update into DStreams using map()
    val statuses = tweets.map(status => status.getText())
    
    // Blow out each word into a new DStream
    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))

    /// English stop words
    val stopWords = List("able","about","above","abroad","according","accordingly","across","actually","adj","after","afterwards","again","against","ago","ahead","ain\u0027t","all","allow","allows","almost","alone","along","alongside","already","also","although","always","am","amid","amidst","among","amongst","an","and","another","any","anybody","anyhow","anyone","anything","anyway","anyways","anywhere","apart","appear","appreciate","appropriate","are","aren\u0027t","around","as","a\u0027s","aside","ask","asking","associated","at","available","away","awfully","back","backward","backwards","be","became","because","become","becomes","becoming","been","before","beforehand","begin","behind","being","believe","below","beside","besides","best","better","between","beyond","both","brief","but","by","came","can","cannot","cant","can\u0027t","caption","cause","causes","certain","certainly","changes","clearly","c\u0027mon","co","co.","com","come","comes","concerning","consequently","consider","considering","contain","containing","contains","corresponding","could","couldn\u0027t","course","c\u0027s","currently","dare","daren\u0027t","definitely","described","despite","did","didn\u0027t","different","directly","do","does","doesn\u0027t","doing","done","don\u0027t","down","downwards","during","each","edu","eg","eight","eighty","either","else","elsewhere","end","ending","enough","entirely","especially","et","etc","even","ever","evermore","every","everybody","everyone","everything","everywhere","ex","exactly","example","except","fairly","far","farther","few","fewer","fifth","first","five","followed","following","follows","for","forever","former","formerly","forth","forward","found","four","from","further","furthermore","get","gets","getting","given","gives","go","goes","going","gone","got","gotten","greetings","had","hadn\u0027t","half","happens","hardly","has","hasn\u0027t","have","haven\u0027t","having","he","he\u0027d","he\u0027ll","hello","help","hence","her","here","hereafter","hereby","herein","here\u0027s","hereupon","hers","herself","he\u0027s","hi","him","himself","his","hither","hopefully","how","howbeit","however","hundred","i\u0027d","ie","if","ignored","i\u0027ll","i\u0027m","immediate","in","inasmuch","inc","inc.","indeed","indicate","indicated","indicates","inner","inside","insofar","instead","into","inward","is","isn\u0027t","it","it\u0027d","it\u0027ll","its","it\u0027s","itself","i\u0027ve","just","k","keep","keeps","kept","know","known","knows","last","lately","later","latter","latterly","least","less","lest","let","let\u0027s","like","liked","likely","likewise","little","look","looking","looks","low","lower","ltd","made","mainly","make","makes","many","may","maybe","mayn\u0027t","me","mean","meantime","meanwhile","merely","might","mightn\u0027t","mine","minus","miss","more","moreover","most","mostly","mr","mrs","much","must","mustn\u0027t","my","myself","name","namely","nd","near","nearly","necessary","need","needn\u0027t","needs","neither","never","neverf","neverless","nevertheless","new","next","nine","ninety","no","nobody","non","none","nonetheless","noone","no-one","nor","normally","not","nothing","notwithstanding","novel","now","nowhere","obviously","of","off","often","oh","ok","okay","old","on","once","one","ones","one\u0027s","only","onto","opposite","or","other","others","otherwise","ought","oughtn\u0027t","our","ours","ourselves","out","outside","over","overall","own","particular","particularly","past","per","perhaps","placed","please","plus","possible","presumably","probably","provided","provides","que","quite","qv","rather","rd","re","really","reasonably","recent","recently","regarding","regardless","regards","relatively","respectively","right","round","said","same","saw","say","saying","says","second","secondly","see","seeing","seem","seemed","seeming","seems","seen","self","selves","sensible","sent","serious","seriously","seven","several","shall","shan\u0027t","she","she\u0027d","she\u0027ll","she\u0027s","should","shouldn\u0027t","since","six","so","some","somebody","someday","somehow","someone","something","sometime","sometimes","somewhat","somewhere","soon","sorry","specified","specify","specifying","still","sub","such","sup","sure","take","taken","taking","tell","tends","th","than","thank","thanks","thanx","that","that\u0027ll","thats","that\u0027s","that\u0027ve","the","their","theirs","them","themselves","then","thence","there","thereafter","thereby","there\u0027d","therefore","therein","there\u0027ll","there\u0027re","theres","there\u0027s","thereupon","there\u0027ve","these","they","they\u0027d","they\u0027ll","they\u0027re","they\u0027ve","thing","things","think","third","thirty","this","thorough","thoroughly","those","though","three","through","throughout","thru","thus","till","to","together","too","took","toward","towards","tried","tries","truly","try","trying","t\u0027s","twice","two","un","under","underneath","undoing","unfortunately","unless","unlike","unlikely","until","unto","up","upon","upwards","us","use","used","useful","uses","using","usually","v","value","various","versus","very","via","viz","vs","want","wants","was","wasn\u0027t","way","we","we\u0027d","welcome","well","we\u0027ll","went","were","we\u0027re","weren\u0027t","we\u0027ve","what","whatever","what\u0027ll","what\u0027s","what\u0027ve","when","whence","whenever","where","whereafter","whereas","whereby","wherein","where\u0027s","whereupon","wherever","whether","which","whichever","while","whilst","whither","who","who\u0027d","whoever","whole","who\u0027ll","whom","whomever","who\u0027s","whose","why","will","willing","wish","with","within","without","wonder","won\u0027t","would","wouldn\u0027t","yes","yet","you","you\u0027d","you\u0027ll","your","you\u0027re","yours","yourself","yourselves","you\u0027ve","zero","a","how\u0027s","i","when\u0027s","why\u0027s","b","c","d","e","f","g","h","j","l","m","n","o","p","q","r","s","t","u","uucp","w","x","y","z","I","www","amount","bill","bottom","call","computer","con","couldnt","cry","de","describe","detail","due","eleven","empty","fifteen","fifty","fill","find","fire","forty","front","full","give","hasnt","herse","himse","interest","itse”","mill","move","myse”","part","put","show","side","sincere","sixty","system","ten","thick","thin","top","twelve","twenty","abst","accordance","act","added","adopted","affected","affecting","affects","ah","announce","anymore","apparently","approximately","aren","arent","arise","auth","beginning","beginnings","begins","biol","briefly","ca","date","ed","effect","et-al","ff","fix","gave","giving","heres","hes","hid","home","id","im","immediately","importance","important","index","information","invention","itd","keys","kg","km","largely","lets","line","\u0027ll","means","mg","million","ml","mug","na","nay","necessarily","nos","noted","obtain","obtained","omitted","ord","owing","page","pages","poorly","possibly","potentially","pp","predominantly","present","previously","primarily","promptly","proud","quickly","ran","readily","ref","refs","related","research","resulted","resulting","results","run","sec","section","shed","shes","showed","shown","showns","shows","significant","significantly","similar","similarly","slightly","somethan","specifically","state","states","stop","strongly","substantially","successfully","sufficiently","suggest","thered","thereof","therere","thereto","theyd","theyre","thou","thoughh","thousand","throug","til","tip","ts","ups","usefully","usefulness","\u0027ve","vol","vols","wed","whats","wheres","whim","whod","whos","widely","words","world","youd","youre")

    // Now eliminate anything that's not a hashtag
    //val hashtags = tweetwords.filter(word => word.startsWith("#"))

    val filteredWords = tweetwords.filter(!stopWords.contains(_))

    // Map each hashtag to a key/value pair of (word, 1) so we can count them up by adding up the values
    val words = filteredWords.map(word => (word, 1))
    
    // Now count them up over a 5 minute window sliding every one second
    val wordCounts = words.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(1))
    //  You will often see this written in the following shorthand:
    //val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( _ + _, _ -_, Seconds(300), Seconds(1))
    
    // Sort the results by the count values
    val sortedResults = wordCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    
    // Print the top 10
    sortedResults.print
    
    // Set a checkpoint directory, and kick it all off
    // I could watch this all day!
    ssc.checkpoint("/Users/alicjamazur/Desktop/all-projects/spark-streaming-course/tweets-streaming/checkpoint4")
    ssc.start()
    ssc.awaitTermination()
  }  
}
