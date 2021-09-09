FROM python:3

# install pandoc
RUN mkdir -p data && \
    cd data/ && \
    wget https://github.com/jgm/pandoc/releases/download/2.14.1/pandoc-2.14.1-1-amd64.deb && \
    dpkg -i pandoc-2.14.1-1-amd64.deb && \
    rm pandoc-2.14.1-1-amd64.deb

# install redmine-zulip
RUN mkdir -p venv-tracker && \
    python3 -m venv /venv-tracker && \
    . /venv-tracker/bin/activate && \
    pip install pip --upgrade

COPY . /data/redmine-zulip/
RUN . /venv-tracker/bin/activate && pip install /data/redmine-zulip/
RUN rm -rf /data/redmine-zulip

CMD ["/venv-tracker/bin/redmine-zulip-publisher", "/data/config.toml"]
