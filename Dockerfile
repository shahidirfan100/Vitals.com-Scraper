# Apify Actor with Playwright Firefox for stealth scraping
FROM apify/actor-node-playwright-firefox:22

# Copy package files
COPY --chown=myuser:myuser package*.json ./

# Install NPM packages, skip optional and development dependencies
RUN npm --quiet set progress=false \
    && npm install --omit=dev --omit=optional \
    && echo "Installed NPM packages:" \
    && (npm list --omit=dev --all || true) \
    && echo "Node.js version:" \
    && node --version \
    && echo "NPM version:" \
    && npm --version \
    && rm -r ~/.npm || true

# Copy source code
COPY --chown=myuser:myuser . ./

# Run the actor
CMD npm start --silent
