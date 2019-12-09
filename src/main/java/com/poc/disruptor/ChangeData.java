package com.poc.disruptor;

import org.apache.commons.lang3.builder.ToStringBuilder;

class ChangeData {

    private final String payload;

    ChangeData(String payload) {
        this.payload = payload;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("payload", payload)
                .toString();
    }
}
