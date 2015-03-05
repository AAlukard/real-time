package ua.sasha.realtime.ranking.tools;

import backtype.storm.tuple.Tuple;
import ua.sasha.realtime.tools.Rankable;

import java.io.Serializable;

/**
 * Created by oleksandr on 3/5/15.
 */
public class RankableTweet implements Rankable, Serializable {

    private static final long serialVersionUID = -9102878650001058090L;
    private static final String toStringSeparator = "|";

    private final String hashtag;
    private final long count;
    private final String tweet;

    public RankableTweet(String hashtag, long count, String tweet) {
        if (hashtag == null) {
            throw new IllegalArgumentException("The hashtag must not be null");
        }
        if (count < 0) {
            throw new IllegalArgumentException("The count must be >= 0");
        }
        this.hashtag = hashtag;
        this.count = count;
        this.tweet = tweet;

    }

    public static RankableTweet from(Tuple tuple) {
        String hashtag = tuple.getString(0);
        Long count = tuple.getLong(1);
        String tweet = tuple.getString(2);
        return new RankableTweet(hashtag, count, tweet);
    }

    public String getObject() {
        return hashtag;
    }

    public long getCount() {
        return count;
    }

    public String getTweet() {
        return tweet;
    }

    @Override
    public int compareTo(Rankable other) {
        long delta = this.getCount() - other.getCount();
        if (delta > 0) {
            return 1;
        }
        else if (delta < 0) {
            return -1;
        }
        else {
            return 0;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RankableTweet)) {
            return false;
        }
        RankableTweet other = (RankableTweet) o;
        return hashtag.equals(other.hashtag) && count == other.count;
    }

    @Override
    public int hashCode() {
        int result = 17;
        int countHash = (int) (count ^ (count >>> 32));
        result = 31 * result + countHash;
        result = 31 * result + hashtag.hashCode();
        return result;
    }

    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("[");
        buf.append(hashtag);
        buf.append(toStringSeparator);
        buf.append(count);

        buf.append(toStringSeparator);
        buf.append(tweet);

        buf.append("]");
        return buf.toString();
    }

    /**
     * Note: We do not defensively copy the wrapped object and any accompanying fields.  We do guarantee, however,
     * do return a defensive (shallow) copy of the List object that is wrapping any accompanying fields.
     *
     * @return
     */
    @Override
    public Rankable copy() {
        return new RankableTweet(getObject(), getCount(), getTweet());
    }
}

