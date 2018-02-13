FROM scratch

ADD ./kaffe

ENTRYPOINT ["/kaffe"]
