FROM python:3

# install redmine-zulip
RUN mkdir -p venv-tracker && \
    python3 -m venv /venv-tracker && \
    . /venv-tracker/bin/activate && \
    pip install pip --upgrade

COPY . /data/redmine-zulip/
RUN . /venv-tracker/bin/activate && pip install /data/redmine-zulip/
RUN rm -rf /data/redmine-zulip

CMD ["/venv-tracker/bin/redmine-zulip-publisher", "/data/config.toml"]
