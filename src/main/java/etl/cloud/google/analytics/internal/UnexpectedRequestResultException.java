package etl.cloud.google.analytics.internal;

public class UnexpectedRequestResultException extends Exception {

    public UnexpectedRequestResultException() {
        super();
    }

    public UnexpectedRequestResultException(String msg) {
        super(msg);
    }

    public UnexpectedRequestResultException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public UnexpectedRequestResultException(Throwable cause){
        super(cause);
    }

}
