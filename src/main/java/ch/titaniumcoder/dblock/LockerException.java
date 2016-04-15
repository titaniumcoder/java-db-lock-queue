package ch.titaniumcoder.dblock;

public class LockerException extends RuntimeException {
    public LockerException() {
    }

    public LockerException(String message) {
        super(message);
    }

    public LockerException(String message, Throwable cause) {
        super(message, cause);
    }

    public LockerException(Throwable cause) {
        super(cause);
    }

    public LockerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
