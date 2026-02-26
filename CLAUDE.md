# CLAUDE.md — Numaflow (Claude Code Edition)

> **Full Policy:** See `AGENTS.md` for comprehensive details.

---

## Security

### Owners & Contact
- **Security Team:** #collab-platform-security on Slack.
- **Manual Review:** Changes to pipeline logic, data handling, API endpoints, K8s manifests, RBAC → request security review.

### Security Invariants
- **[MUST] No secrets in code or logs** (credentials, tokens, API keys).
- **[MUST] Validate all pipeline inputs** — sanitize untrusted data.
- **[MUST] Data isolation between pipelines** — no cross-pipeline leakage.
- **[MUST] Pin supply chain:** Go deps→exact versions, images→version/SHA.
- **[MUST] All code in approved GitHub organizations** (AtlanHQ).

### Secret Discovery Protocol
Secret found → **CRITICAL**. Do NOT commit. Flag with 🔒 SECURITY REVIEW. Recommend rotation. Notify Security.

### Key Rules
- **[MUST]** Resource limits on all containers/vertices
- **[MUST]** Auth on all API endpoints; generic error messages
- **[MUST]** Container: `runAsNonRoot`, `readOnlyRootFilesystem`, `drop: ["ALL"]`
- **[REDLINE]** Processing untrusted data without validation; `privileged: true`

### Security Checklist
- [ ] No secrets in code/config/logs
- [ ] Pipeline inputs validated
- [ ] Data isolation enforced
- [ ] Auth on all endpoints
- [ ] Resource limits set
- [ ] Container non-root with minimal capabilities
- [ ] Dependencies and images pinned
- [ ] SCA scanning (Snyk) configured

> **Full details:** See `AGENTS.md § Security`.
